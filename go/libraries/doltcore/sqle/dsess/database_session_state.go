// Copyright 2020 Dolthub, Inc.
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

package dsess

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// InitialDbState is the initial state of a database, as returned by SessionDatabase.InitialDBState. It is used to
// establish the in memory state of the session for every new transaction.
type InitialDbState struct {
	Db sql.Database
	// WorkingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case HeadCommit must be set
	WorkingSet *doltdb.WorkingSet
	// The head commit for this database. May be nil for databases tied to a detached root value, in which case
	// RootValue must be set.
	HeadCommit *doltdb.Commit
	// HeadRoot is the root value for databases without a HeadCommit. Nil for databases with a HeadCommit.
	HeadRoot    *doltdb.RootValue
	ReadOnly    bool
	DbData      env.DbData
	Remotes     map[string]env.Remote
	Branches    map[string]env.BranchConfig
	Backups     map[string]env.Remote

	// If err is set, this InitialDbState is partially invalid, but may be
	// usable to initialize a database at a revision specifier, for
	// example. Adding this InitialDbState to a session will return this
	// error.
	Err error
}

// SessionDatabase is a database that can be managed by a dsess.Session. It has methods to return its initial state in
// order for the session to manage it.
type SessionDatabase interface {
	sql.Database
	InitialDBState(ctx *sql.Context, branch string) (InitialDbState, error)
}

// DatabaseSessionState is the set of all information for a given database in this session.
type DatabaseSessionState struct {
	// dbName is the name of the database this state applies to. This includes a revision specifier in some cases.
	dbName       string
	// db is the database this state applies to
	db           SqlDatabase
	// currentHead is the current head of the database when unqualified by a DB name
	currentHead ref.WorkingSetRef
	// heads records the in-memory DB state for every branch head accessed by the session
	heads 			map[string]*branchState
	// globalState is the global state of this session (shared by all sessions for a particular db)
	globalState  globalstate.GlobalState
	// dirty is true if this session has uncommitted changes
	dirty        bool
	// tmpFileDir is the directory to use for temporary files for this database
	tmpFileDir   string

	// sessionCache is a collection of cached values used to speed up performance
	sessionCache *SessionCache

	// Same as InitialDbState.Err, this signifies that this
	// DatabaseSessionState is invalid. LookupDbState returning a
	// DatabaseSessionState with Err != nil will return that err.
	Err error
}

type SessionState interface {
	GetWorkingSet() *doltdb.WorkingSet
	GetWriteSession() writer.WriteSession
}

func (d *branchState) GetWorkingSet() *doltdb.WorkingSet {
	return d.workingSet
}

func (d *branchState) GetWriteSession() writer.WriteSession {
	return d.writeSession
}

// branchState records all the in-memory session state for a particular branch head
type branchState struct {
	// headCommit is the head commit for this database. May be nil for databases tied to a detached root value, in which 
	// case headRoot must be set.
	headCommit   *doltdb.Commit
	// HeadRoot is the root value for databases without a headCommit. Nil for databases with a headCommit.
	headRoot     *doltdb.RootValue
	// workingSet is the working set for this database. May be nil for databases tied to a detached root value, in which
	// case headCommit must be set
	workingSet *doltdb.WorkingSet
	// dbData is an accessor for the underlying doltDb
	dbData       env.DbData
	// writeSession is this database's write session, which changes when the working set changes
	writeSession writer.WriteSession
	// globalState is the global state of this session (shared by all sessions for a particular db)
	// readOnly is true if this database is read only
	readOnly     bool
}

func NewEmptyDatabaseSessionState() *DatabaseSessionState {
	return &DatabaseSessionState{
		sessionCache: newSessionCache(),
		heads: make(map[ref.WorkingSetRef]branchState),
		// TODO: current head?
	}
}

func (d DatabaseSessionState) GetRoots() doltdb.Roots {
	if d.GetWorkingSet(ctx) == nil {
		return doltdb.Roots{
			Head:    d.headRoot,
			Working: d.headRoot,
			Staged:  d.headRoot,
		}
	}
	return doltdb.Roots{
		Head:    d.headRoot,
		Working: d.GetWorkingSet(ctx).WorkingRoot(),
		Staged:  d.GetWorkingSet(ctx).StagedRoot(),
	}
}

func (d *DatabaseSessionState) SessionCache() *SessionCache {
	return d.sessionCache
}

func (d DatabaseSessionState) EditOpts() editor.Options {
	return d.GetWriteSession().GetOptions()
}
