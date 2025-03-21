// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statspro

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
)

const (
	boostrapRowLimit = 2e6
)

func (p *Provider) RefreshTableStats(ctx *sql.Context, table sql.Table, db string) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	branch, err := dSess.GetBranch()
	if err != nil {
		return err
	}
	return p.RefreshTableStatsWithBranch(ctx, table, db, branch)
}

func (p *Provider) BootstrapDatabaseStats(ctx *sql.Context, db string) error {
	dSess := dsess.DSessFromSess(ctx.Session)
	branches := p.getStatsBranches(ctx)
	var rows uint64
	for _, branch := range branches {
		sqlDb, err := dSess.Provider().Database(ctx, BranchQualifiedDatabase(db, branch))
		if err != nil {
			if sql.ErrDatabaseNotFound.Is(err) {
				// default branch is not valid
				continue
			}
			return err
		}
		tables, err := sqlDb.GetTableNames(ctx)
		if err != nil {
			return err
		}
		for _, table := range tables {
			sqlTable, _, err := GetLatestTable(ctx, table, sqlDb)
			if err != nil {
				return err
			}

			if st, ok := sqlTable.(sql.StatisticsTable); ok {
				cnt, ok, err := st.RowCount(ctx)
				if ok && err == nil {
					rows += cnt
				}
			}
			if rows >= boostrapRowLimit {
				return fmt.Errorf("stats bootstrap aborted because %s exceeds the default row limit; manually run \"ANALYZE <table>\" or \"call dolt_stats_restart()\" to collect statistics", db)
			}

			if err := p.RefreshTableStatsWithBranch(ctx, sqlTable, db, branch); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Provider) RefreshTableStatsWithBranch(ctx *sql.Context, table sql.Table, db string, branch string) error {
	if !p.TryLockForUpdate(branch, db, table.Name()) {
		return fmt.Errorf("already updating statistics")
	}
	defer p.UnlockTable(branch, db, table.Name())

	dSess := dsess.DSessFromSess(ctx.Session)

	sqlDb, err := dSess.Provider().Database(ctx, BranchQualifiedDatabase(db, branch))
	if err != nil {
		return err
	}

	// lock only after accessing DatabaseProvider

	tableName := strings.ToLower(table.Name())
	dbName := strings.ToLower(db)
	var schemaName string
	if schTab, ok := table.(sql.DatabaseSchemaTable); ok {
		schemaName = strings.ToLower(schTab.DatabaseSchema().SchemaName())
	}

	iat, ok := table.(sql.IndexAddressableTable)
	if !ok {
		return nil
	}
	indexes, err := iat.GetIndexes(ctx)
	if err != nil {
		return err
	}

	// it's important to update WORKING session references every call
	sqlTable, dTab, err := GetLatestTable(ctx, tableName, sqlDb)
	if err != nil {
		return err
	}

	statDb, ok := p.getStatDb(dbName)
	if !ok {
		// if the stats database does not exist, initialize one
		fs, err := p.pro.FileSystemForDatabase(dbName)
		if err != nil {
			return err
		}
		sourceDb, ok := p.pro.BaseDatabase(ctx, dbName)
		if !ok {
			return sql.ErrDatabaseNotFound.New(dbName)
		}
		statDb, err = p.sf.Init(ctx, sourceDb, p.pro, fs, env.GetCurrentUserHomeDir)
		if err != nil {
			ctx.Warn(0, "%s", err.Error())
			return nil
		}
		p.setStatDb(dbName, statDb)
	}

	schHash, err := dTab.GetSchemaHash(ctx)
	if err != nil {
		return err
	}

	if oldSchHash, err := statDb.GetSchemaHash(ctx, branch, tableName); oldSchHash.IsEmpty() {
		if err := statDb.SetSchemaHash(ctx, branch, tableName, schHash); err != nil {
			return fmt.Errorf("set schema hash error: %w", err)
		}
	} else if oldSchHash != schHash {
		ctx.GetLogger().Debugf("statistics refresh: detected table schema change: %s,%s/%s", dbName, table, branch)
		if err := statDb.SetSchemaHash(ctx, branch, tableName, schHash); err != nil {
			return err
		}

		stats, err := p.GetTableDoltStats(ctx, branch, dbName, schemaName, tableName)
		if err != nil {
			return err
		}
		for _, stat := range stats {
			statDb.DeleteStats(ctx, branch, stat.Qualifier())
		}
	} else if err != nil {
		return err
	}

	tablePrefix := fmt.Sprintf("%s.", tableName)
	var idxMetas []indexMeta
	for _, idx := range indexes {
		cols := make([]string, len(idx.Expressions()))
		for i, c := range idx.Expressions() {
			cols[i] = strings.TrimPrefix(strings.ToLower(c), tablePrefix)
		}

		qual := sql.NewStatQualifier(db, schemaName, table.Name(), strings.ToLower(idx.ID()))
		curStat, ok := statDb.GetStat(branch, qual)
		if !ok {
			curStat = NewDoltStats()
			curStat.Statistic.Qual = qual
		}
		idxMeta, ok, err := newIdxMeta(ctx, curStat, dTab, idx, cols)
		if err != nil {
			return err
		}
		if ok {
			idxMetas = append(idxMetas, idxMeta)
		}
	}

	newTableStats, err := createNewStatsBuckets(ctx, sqlTable, dTab, indexes, idxMetas)
	if err != nil {
		return err
	}

	// merge new chunks with preexisting chunks
	for _, idxMeta := range idxMetas {
		stat, ok := newTableStats[idxMeta.qual]
		if !ok {
			continue
		}
		targetChunks, err := MergeNewChunks(idxMeta.allAddrs, idxMeta.keepChunks, stat.Hist)
		if err != nil {
			return err
		}
		if targetChunks == nil {
			// empty table
			continue
		}
		stat.SetChunks(idxMeta.allAddrs)
		stat.Hist = targetChunks
		stat.UpdateActive()
		if err := statDb.SetStat(ctx, branch, idxMeta.qual, stat); err != nil {
			return err
		}
	}

	p.UpdateStatus(dbName, fmt.Sprintf("refreshed %s", dbName))
	return statDb.Flush(ctx, branch)
}

// BranchQualifiedDatabase returns a branch qualified database. If the database
// is already branch suffixed no duplication is applied.
func BranchQualifiedDatabase(db, branch string) string {
	suffix := fmt.Sprintf("/%s", branch)
	if !strings.HasSuffix(db, suffix) {
		return fmt.Sprintf("%s%s", db, suffix)
	}
	return db
}

// GetLatestTable will get the WORKING root table for the current database/branch
func GetLatestTable(ctx *sql.Context, tableName string, sqlDb sql.Database) (sql.Table, *doltdb.Table, error) {
	var db sqle.Database
	switch d := sqlDb.(type) {
	case sqle.Database:
		db = d
	case sqle.ReadReplicaDatabase:
		db = d.Database
	default:
		return nil, nil, fmt.Errorf("expected sqle.Database, found %T", sqlDb)
	}
	sqlTable, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("statistics refresh error: table not found %s", tableName)
	}

	var dTab *doltdb.Table
	switch t := sqlTable.(type) {
	case *sqle.AlterableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.WritableDoltTable:
		dTab, err = t.DoltTable.DoltTable(ctx)
	case *sqle.DoltTable:
		dTab, err = t.DoltTable(ctx)
	default:
		err = fmt.Errorf("failed to unwrap dolt table from type: %T", sqlTable)
	}
	if err != nil {
		return nil, nil, err
	}
	return sqlTable, dTab, nil
}

func newIdxMeta(ctx *sql.Context, curStats *DoltStats, doltTable *doltdb.Table, sqlIndex sql.Index, cols []string) (indexMeta, bool, error) {
	var idx durable.Index
	var err error
	if strings.EqualFold(sqlIndex.ID(), "PRIMARY") {
		idx, err = doltTable.GetRowData(ctx)
	} else {
		idx, err = doltTable.GetIndexRowData(ctx, sqlIndex.ID())
	}
	if err != nil {
		return indexMeta{}, false, err
	}

	prollyMap, ok := durable.MaybeProllyMapFromIndex(idx)
	if !ok {
		return indexMeta{}, false, nil
	}

	if cnt, err := prollyMap.Count(); err != nil {
		return indexMeta{}, false, err
	} else if cnt == 0 {
		return indexMeta{
			qual: curStats.Statistic.Qual,
			cols: cols,
		}, true, nil
	}

	// get newest histogram target level hashes
	levelNodes, err := tree.GetHistogramLevel(ctx, prollyMap.Tuples(), bucketLowCnt)
	if err != nil {
		return indexMeta{}, false, err
	}

	var addrs []hash.Hash
	var keepChunks []sql.HistogramBucket
	var missingAddrs float64
	var missingChunks []tree.Node
	var missingOffsets []updateOrdinal
	var offset uint64

	for _, n := range levelNodes {
		// Compare the previous histogram chunks to the newest tree chunks.
		// Partition the newest chunks into 1) preserved or 2) missing.
		// Missing chunks will need to be scanned on a stats update, so
		// track the (start, end) ordinal offsets to simplify the read iter.
		treeCnt, err := n.TreeCount()
		if err != nil {
			return indexMeta{}, false, err
		}

		addrs = append(addrs, n.HashOf())
		if bucketIdx, ok := curStats.Active[n.HashOf()]; !ok {
			missingChunks = append(missingChunks, n)
			missingOffsets = append(missingOffsets, updateOrdinal{offset, offset + uint64(treeCnt)})
			missingAddrs++
		} else {
			keepChunks = append(keepChunks, curStats.Hist[bucketIdx])
		}
		offset += uint64(treeCnt)
	}

	var dropChunks []sql.HistogramBucket
	for _, h := range curStats.Chunks {
		var match bool
		for _, b := range keepChunks {
			if DoltBucketChunk(b) == h {
				match = true
				break
			}
		}
		if !match {
			dropChunks = append(dropChunks, curStats.Hist[curStats.Active[h]])
		}
	}

	return indexMeta{
		qual:           curStats.Statistic.Qual,
		cols:           cols,
		newNodes:       missingChunks,
		updateOrdinals: missingOffsets,
		keepChunks:     keepChunks,
		dropChunks:     dropChunks,
		allAddrs:       addrs,
	}, true, nil
}
