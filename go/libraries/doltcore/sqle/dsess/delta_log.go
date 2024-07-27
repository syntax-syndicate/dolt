package dsess

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/store/hash"
)

type DeltaLog struct {
	mu *sync.RWMutex
	// init/remove db hooks
	// change default branch expressed as drop/add?
	dbDeltas []dbDelta
	// head updates
	dbLogs        map[string]dbHeads
	branchChanges []string
	doReadRepl    atomic.Bool
}

func NewDeltaLog(dbs []SqlDatabase) (*DeltaLog, error) {
	dbLogs := make(map[string]dbHeads)
	for _, db := range dbs {
		dbName := strings.ToLower(db.Name())
		baseName, revName := SplitRevisionDbName(dbName)
		head := revName
		if head == "" {
			_, val, ok := sql.SystemVariables.GetGlobal(DefaultBranchKey(baseName))
			if ok {
				head = val.(string)
				branchRef, err := ref.Parse(head)
				if err == nil {
					head = branchRef.GetPath()
				} else {
					head = ""
					// continue to below
				}
			}
		}
		if head == "" {
			rsr := db.DbData().Rsr
			if rsr != nil {
				headRef, err := rsr.CWBHeadRef()
				if err != nil {
					return nil, err
				}
				head = headRef.GetPath()
			}
		}

		//dbState, err := db.InitialDBState(ctx)

		dbHead := dbHeads{branch: head}
		dbLogs[baseName] = dbHead
		dbLogs[dbName] = dbHead

	}
	return &DeltaLog{dbLogs: dbLogs}, nil

	//dbLogs := make(map[string]dbHeads)
	//for _, db := range dbs {
	//	// revision name
	//	baseName, revName := SplitRevisionDbName(db.Name())
	//
	//	// initial state
	//	dbState, err := db.InitialDBState(ctx)
	//
	//	// default head
	//	if head == "" {
	//		rsr := db.DbData().Rsr
	//		if rsr != nil {
	//			headRef, err := rsr.CWBHeadRef()
	//			if err != nil {
	//				return "", err
	//			}
	//			head = headRef.GetPath()
	//		}
	//	}
	//
	//	if head == "" {
	//		head = db.Revision()
	//	}
	//
	//	dbLogs[strings.ToLower(db.Name())] = &dbState{head: root., working:, staged: }
	//}
	//return &DeltaLog{dbLogs: make(map[string]dbState)}
}

type dbDelta struct {
	// add or delete database
	add         bool
	displayName string
	baseName    string
	revName     string
	db          SqlDatabase // *doltdb.DoltDB
}

type dbHeads struct {
	working, staged, head hash.Hash
	branch                string
}

func (d *DeltaLog) AddDatabase(name string, db SqlDatabase) {

}

func (d *DeltaLog) DropDatabase(name string) {

}

func (d *DeltaLog) changeBranch() {

}

func (d *DeltaLog) newRoot() {

}

func (d *DeltaLog) setReadRepl(b bool) {

}
