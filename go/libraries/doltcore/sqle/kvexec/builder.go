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

package kvexec

import (
	"context"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type Builder struct{}

var _ sql.NodeExecBuilder = (*Builder)(nil)

func (b Builder) Build(ctx *sql.Context, n sql.Node, r sql.Row) (sql.RowIter, error) {

	// TODO: join optimization limits should be relaxed:
	//  - expression types supported
	//  - filter hoist levels
	//  - parent row index shifts
	//  - fusing kvexec operators
	//  - compatible |val| encodings that we don't coerce

	switch n := n.(type) {
	case *plan.JoinNode:
		if n.Op.IsLookup() && !n.Op.IsPartial() {
			if ita, ok := getIta(n.Right()); ok && len(r) == 0 && simpleLookupExpressions(ita.Expressions()) {
				if _, _, _, dstIter, _, _, dstTags, dstFilter, err := getSourceKv(ctx, n.Right(), false); err == nil && dstIter != nil {
					if srcMap, _, srcIter, _, srcSchema, _, srcTags, srcFilter, err := getSourceKv(ctx, n.Left(), true); err == nil && srcSchema != nil {
						if keyLookupMapper := newLookupKeyMapping(ctx, srcSchema, dstIter.InputKeyDesc(), ita.Expressions(), srcMap.NodeStore()); keyLookupMapper.valid() {
							// conditions:
							// (1) lookup or left lookup join
							// (2) left-side is something we read KVs from (table or indexscan, ex: no subqueries)
							// (3) right-side is an index lookup, by definition
							// (4) the key expressions for the lookup are literals or columns (ex: no arithmetic yet)
							split := len(srcTags)
							projections := append(srcTags, dstTags...)
							rowJoiner := newRowJoiner([]schema.Schema{srcSchema, dstIter.Schema()}, []int{split}, projections, dstIter.NodeStore())
							return newLookupKvIter(srcIter, dstIter, keyLookupMapper, rowJoiner, srcFilter, dstFilter, n.Filter, n.Op.IsLeftOuter(), n.Op.IsExcludeNulls())
						}
					}
				}
			}
		}
		if n.Op.IsMerge() && !n.Op.IsPartial() && len(r) == 0 {
			if leftMap, leftIter, lPriSch, lSecSch, leftTags, leftFilter, leftNorm, err := getMergeKv(ctx, n.Left()); err == nil {
				if rightMap, rightIter, rPriSch, rSecSch, rightTags, rightFilter, rightNorm, err := getMergeKv(ctx, n.Right()); err == nil {
					filters := expression.SplitConjunction(n.Filter)
					projections := append(leftTags, rightTags...)
					// - secondary indexes are source of comparison columns.
					// - usually key tuple, but for keyless tables it's val tuple.
					// - use primary table projections as reference for comparison
					//   filter indexes.
					if cmp, ok := mergeComparer(filters[0], lSecSch, rSecSch, projections, leftMap.KeyDesc(), leftMap.ValDesc(), rightMap.KeyDesc(), rightMap.ValDesc()); ok {
						split := len(leftTags)
						var rowJoiner *prollyToSqlJoiner
						rowJoiner = newRowJoiner([]schema.Schema{lPriSch, rPriSch}, []int{split}, projections, leftMap.NodeStore())
						if iter, err := newMergeKvIter(leftIter, rightIter, rowJoiner, cmp, leftNorm, rightNorm, leftFilter, rightFilter, filters, n.Op.IsLeftOuter(), n.Op.IsExcludeNulls()); err == nil {
							return iter, nil
						}
					}
				}
			}
		}
	case *plan.GroupBy:
		if len(n.GroupByExprs) == 0 && len(n.SelectedExprs) == 1 {
			if cnt, ok := n.SelectedExprs[0].(*aggregation.Count); ok {
				if _, _, srcIter, _, srcSchema, _, _, srcFilter, err := getSourceKv(ctx, n.Child, true); err == nil && srcSchema != nil && srcFilter == nil {
					iter, ok, err := newCountAggregationKvIter(srcIter, srcSchema, cnt.Child)
					if ok && err == nil {
						// (1) no grouping expressions (returns one row)
						// (2) only one COUNT expression with a literal or field reference
						// (3) table or ita as child (no filters)
						return iter, nil
					}
				}
			}
		}
	default:
	}
	return nil, nil
}

func getIta(n sql.Node) (*plan.IndexedTableAccess, bool) {
	switch n := n.(type) {
	case *plan.TableAlias:
		return getIta(n.Child)
	case *plan.Filter:
		return getIta(n.Child)
	case *plan.IndexedTableAccess:
		return n, true
	default:
		return nil, false
	}
}

// simpleLookupExpressions returns true if |keyExprs| includes only field
// references and literals
func simpleLookupExpressions(keyExprs []sql.Expression) bool {
	for _, e := range keyExprs {
		switch e.(type) {
		case *expression.Literal, *expression.GetField:
		default:
			return false
		}
	}
	return true
}

// prollyToSqlJoiner converts a list of KV pairs into a sql.Row
type prollyToSqlJoiner struct {
	ns tree.NodeStore
	// kvSplits are offsets between consecutive kv pairs
	kvSplits    []int
	desc        []kvDesc
	ordMappings []int
}

type kvDesc struct {
	keyDesc     val.TupleDesc
	valDesc     val.TupleDesc
	keyMappings []int
	valMappings []int
}

func newRowJoiner(schemas []schema.Schema, splits []int, projections []uint64, ns tree.NodeStore) *prollyToSqlJoiner {
	numPhysicalColumns := getPhysicalColCount(schemas, splits, projections)

	// last kv pair can safely look ahead for its end range
	splits = append(splits, len(projections))

	// | k1 | v1 | k2 | v2 | ... | ords |
	// refer to more detailed comment below
	// todo: is it worth refactoring from a two-phase to one-phase mapping?
	allMap := make([]int, 2*numPhysicalColumns)
	var tupleDesc []kvDesc

	nextKeyIdx := 0
	nextValIdx := splits[0] - 1
	sch := schemas[0]
	keylessOff := 0
	if schema.IsKeyless(sch) {
		keylessOff = 1
	}
	keyCols := sch.GetPKCols()
	valCols := sch.GetNonPKCols()
	splitIdx := 0
	for i := 0; i <= len(projections); i++ {
		// We will fill the map from table sources incrementally. Each source will have
		// a keyMapping, valueMapping, and ordinal mappings related to converting from
		// storage order->schema order->projection order. allMap is a shared underlying
		// storage for all of these mappings. Split indexes refers to a K/V segmentation
		// of columns from a table. We increment the key mapping positions and decrement
		// the value mapping positions, so the split index will be where the key and value
		// indexes converge after processing a table source's fields.
		if i == splits[splitIdx] {
			var mappingStartIdx int
			if splitIdx > 0 {
				mappingStartIdx = splits[splitIdx-1]
			}
			tupleDesc = append(tupleDesc, kvDesc{
				keyDesc:     sch.GetKeyDescriptor(),
				valDesc:     sch.GetValueDescriptor(),
				keyMappings: allMap[mappingStartIdx:nextKeyIdx],  // prev kv partition -> last key of this partition
				valMappings: allMap[nextKeyIdx:splits[splitIdx]], // first val of partition -> next kv partition
			})
			if i == len(projections) {
				break
			}
			nextKeyIdx = splits[splitIdx]
			splitIdx++
			nextValIdx = splits[splitIdx] - 1
			sch = schemas[splitIdx]

			keylessOff = 0
			if schema.IsKeyless(sch) {
				keylessOff = 1
			}
			keyCols = sch.GetPKCols()
			valCols = sch.GetNonPKCols()
		}
		tag := projections[i]
		if idx, ok := keyCols.StoredIndexByTag(tag); ok && !keyCols.GetByStoredIndex(idx).Virtual {
			allMap[nextKeyIdx] = idx
			allMap[numPhysicalColumns+nextKeyIdx] = i
			nextKeyIdx++
		} else if idx, ok := valCols.StoredIndexByTag(tag); ok && !valCols.GetByStoredIndex(idx).Virtual {
			allMap[nextValIdx] = idx + keylessOff
			allMap[numPhysicalColumns+nextValIdx] = i
			nextValIdx--
		}
	}

	return &prollyToSqlJoiner{
		kvSplits:    splits,
		desc:        tupleDesc,
		ordMappings: allMap[numPhysicalColumns:],
		ns:          ns,
	}
}

func (m *prollyToSqlJoiner) buildRow(ctx context.Context, tuples ...val.Tuple) (sql.Row, error) {
	if len(tuples) != 2*len(m.desc) {
		panic("invalid KV count for prollyToSqlJoiner")
	}
	row := make(sql.Row, len(m.ordMappings))
	split := 0
	var err error
	var tup val.Tuple
	for i, desc := range m.desc {
		tup = tuples[2*i]
		if tup == nil {
			// nullified row
			split = m.kvSplits[i]
			continue
		}
		if i > 0 {
			split = m.kvSplits[i-1]
		}
		for j, idx := range desc.keyMappings {
			outputIdx := m.ordMappings[split+j]
			row[outputIdx], err = tree.GetField(ctx, desc.keyDesc, idx, tup, m.ns)
			if err != nil {
				return nil, err
			}
		}
		tup = tuples[2*i+1]
		for j, idx := range desc.valMappings {
			outputIdx := m.ordMappings[split+len(desc.keyMappings)+j]
			row[outputIdx], err = tree.GetField(ctx, desc.valDesc, idx, tup, m.ns)
			if err != nil {
				return nil, err
			}
		}
	}
	return row, nil
}

func getPhysicalColCount(schemas []schema.Schema, splits []int, projections []uint64) int {
	var virtual bool
	for _, sch := range schemas {
		if schema.IsVirtual(sch) {
			virtual = true
		}
	}

	if !virtual {
		return len(projections)
	}

	numPhysicalColumns := 0
	sch := schemas[0]
	splitIdx := 0
	for i := 0; i < len(projections); i++ {
		if i == splits[splitIdx] {
			splitIdx++
			sch = schemas[splitIdx]
		}
		tag := projections[i]
		if idx, ok := sch.GetAllCols().TagToIdx[tag]; ok && !sch.GetAllCols().GetByIndex(idx).Virtual {
			numPhysicalColumns++
		}
	}
	return numPhysicalColumns
}

// getSourceKv extracts prolly table and index specific structures needed
// to implement a lookup join. We return either |srcIter| or |dstIter|
// depending on whether |isSrc| is true.
func getSourceKv(ctx *sql.Context, n sql.Node, isSrc bool) (prolly.Map, prolly.Map, prolly.MapIter, index.SecondaryLookupIterGen, schema.Schema, schema.Schema, []uint64, sql.Expression, error) {
	var table *doltdb.Table
	var tags []uint64
	var err error
	var priMap prolly.Map
	var secMap prolly.Map
	var srcIter prolly.MapIter
	var dstIter index.SecondaryLookupIterGen
	var priSch schema.Schema
	switch n := n.(type) {
	case *plan.TableAlias:
		return getSourceKv(ctx, n.Child, isSrc)
	case *plan.Filter:
		m, secM, mIter, destIter, s, _, t, _, err := getSourceKv(ctx, n.Child, isSrc)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		return m, secM, mIter, destIter, s, nil, t, n.Expression, nil
	case *plan.IndexedTableAccess:
		var lb index.IndexScanBuilder
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableIndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}
			lb, err = dt.LookupBuilder(ctx)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}
		case *sqle.IndexedDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}
			lb, err = dt.LookupBuilder(ctx)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}
		//case *dtables.DiffTable:
		// TODO: add interface to include system tables
		default:
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, nil
		}

		rowData, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		priMap = durable.ProllyMapFromIndex(rowData)

		priSch = lb.OutputSchema()

		if isSrc {
			l, err := n.GetLookup(ctx, nil)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}

			prollyRanges, err := index.ProllyRangesForIndex(ctx, l.Index, l.Ranges)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}

			srcIter, err = index.NewSequenceRangeIter(ctx, lb, prollyRanges, l.IsReverse)
			if err != nil {
				return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
			}
		} else {
			dstIter = lb.NewSecondaryIter(n.IsStrictLookup(), len(n.Expressions()), n.NullMask())
		}

	case *plan.ResolvedTable:
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
		case *sqle.AlterableDoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable.DoltTable(ctx)
		case *sqle.DoltTable:
			tags = dt.ProjectedTags()
			table, err = dt.DoltTable(ctx)
		default:
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, nil
		}
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		priSch, err = table.GetSchema(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		priIndex, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		priMap = durable.ProllyMapFromIndex(priIndex)
		secMap = priMap

		srcIter, err = priMap.IterAll(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		if schema.IsKeyless(priSch) {
			srcIter = index.NewKeylessCardedMapIter(srcIter)
		}

	default:
		return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, nil
	}
	if err != nil {
		return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
	}

	if priSch == nil && table != nil {
		priSch, err = table.GetSchema(ctx)
		if err != nil {
			return prolly.Map{}, prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
	}

	return priMap, secMap, srcIter, dstIter, priSch, nil, tags, nil, nil
}

type coveringNormalizer func(val.Tuple) (val.Tuple, val.Tuple, error)

func getMergeKv(ctx *sql.Context, n sql.Node) (prolly.Map, prolly.MapIter, schema.Schema, schema.Schema, []uint64, sql.Expression, coveringNormalizer, error) {
	// merge kv is different from lookup KV:
	// - embed the non-coverign lookup at the iter layer
	// - the map and schema will depend on covering

	// one schema for merge comparison
	// other schema for row joiner

	// covering normalizer to primary key/val

	var m prolly.Map
	var table *doltdb.Table
	var tags []uint64
	var iter prolly.MapIter
	var covering bool
	var priSch schema.Schema
	var idxSch schema.Schema
	var idx index.DoltIndex
	var err error

	switch n := n.(type) {
	case *plan.TableAlias:
		return getMergeKv(ctx, n.Child)
	case *plan.Filter:
		m, mIter, destIter, s, t, _, norm, err := getMergeKv(ctx, n.Child)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		return m, mIter, destIter, s, t, n.Expression, norm, nil
	case *plan.Project:
		m, mIter, destIter, s, t, expr, norm, err := getMergeKv(ctx, n.Child)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		var newTags []uint64
		for _, e := range n.Projections {
			switch e := e.(type) {
			case *expression.GetField:
				newTags = append(newTags, t[e.Index()])
			default:
				return prolly.Map{}, nil, nil, nil, nil, nil, nil, fmt.Errorf("unsupported kvmerge projection")
			}
		}
		return m, mIter, destIter, s, t, expr, norm, nil
	case *plan.IndexedTableAccess:
		var doltTable *sqle.DoltTable
		switch dt := n.UnderlyingTable().(type) {
		case *sqle.WritableIndexedDoltTable:
			idx = dt.Index()
			doltTable = dt.DoltTable
		case *sqle.IndexedDoltTable:
			idx = dt.Index()
			doltTable = dt.DoltTable

		//case *dtables.DiffTable:
		// TODO: add interface to include system tables
		default:
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, nil
		}

		m, err = index.MapForTableIndex(ctx, doltTable, idx)
		table, err = doltTable.DoltTable(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}
		tags = doltTable.ProjectedTags()
		idxSch = idx.IndexSchema()
		priSch = idx.Schema()
		covering = idx.ID() == "primary" || schemaIsCovering(idx.IndexSchema(), tags)
		l, err := n.GetLookup(ctx, nil)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		prollyRanges, err := index.ProllyRangesForIndex(ctx, l.Index, l.Ranges)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		iter, err = index.NewSequenceRangeIter(ctx, index.NewSecondaryIterGen(m), prollyRanges, l.IsReverse)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		if covering {
			// projections satisfied by idxSch
			return m, iter, idxSch, idxSch, tags, nil, nil, nil
		}

		priIndex, err := table.GetRowData(ctx)
		if err != nil {
			return prolly.Map{}, nil, nil, nil, nil, nil, nil, err
		}

		priMap := durable.ProllyMapFromIndex(priIndex)
		pkMap := index.OrdinalMappingFromIndex(idx)
		priKd, _ := priMap.Descriptors()
		pkBld := val.NewTupleBuilder(priKd)

		var covNorm coveringNormalizer = func(key val.Tuple) (val.Tuple, val.Tuple, error) {
			for to := range pkMap {
				from := pkMap.MapOrdinal(to)
				pkBld.PutRaw(to, m.KeyDesc().GetField(from, key))
			}
			pk := pkBld.Build(m.Pool())
			var v val.Tuple
			err = priMap.Get(ctx, pk, func(key val.Tuple, value val.Tuple) error {
				v = value
				return nil
			})
			if err != nil {
				return nil, nil, err
			}
			return pk, v, nil
		}
		return m, iter, priSch, idxSch, tags, nil, covNorm, nil
	default:
		return prolly.Map{}, nil, nil, nil, nil, nil, nil, fmt.Errorf("unsupported kvmerge child node")
	}
}
