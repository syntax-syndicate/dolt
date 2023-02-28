// Copyright 2023 Dolthub, Inc.
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

package binlogreplication

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"sync/atomic"
)

// binlogEventProducer is responsible for reading binlog events from an established connection and sending them back to
// a consumer over a channel. This is necessary because calls to conn.ReadBinlogEvent() block until a binlog event is
// received. If the source isn't sending more events, then the applier is blocked on reading events, and the user
// can't issue a call to STOP REPLICA. Reading binlog events in a thread and communicating with the applier via
// channels for events and errors decouples this.
type binlogEventProducer struct {
	conn      *mysql.Conn
	errorChan chan error
	eventChan chan mysql.BinlogEvent
	running   atomic.Bool
}

// newBinlogEventProducer creates a new binlog event producer that reads from the specified, established MySQL
// connection |conn|, sends events over the |eventChan| channel, and sends any errors over the |errorChan| channel.
func newBinlogEventProducer(conn *mysql.Conn, eventChan chan mysql.BinlogEvent, errorChan chan error) *binlogEventProducer {
	producer := &binlogEventProducer{
		conn:      conn,
		eventChan: eventChan,
		errorChan: errorChan,
	}
	producer.running.Store(true)
	return producer
}

// Go starts this binlogEventProducer in a new goroutine
func (p *binlogEventProducer) Go(_ *sql.Context) {
	go func() {
		for p.running.Load() {
			// ReadBinlogEvent blocks until a binlog event can be read and returned, so this has to be done on a
			// separate thread, otherwise the applier would be blocked and wouldn't be able to handle the STOP
			// REPLICA signal.
			event, err := p.conn.ReadBinlogEvent()

			// If this binlogEventProducer has been stopped while we were blocked waiting to read the next
			// binlog event, abort processing it and just return instead.
			if p.IsRunning() == false {
				return
			}

			if err != nil {
				p.errorChan <- err
			} else {
				p.eventChan <- event
			}
		}
	}()
}

// IsRunning returns true if this instance is processing binlog events and has not been stopped.
func (p *binlogEventProducer) IsRunning() bool {
	return p.running.Load()
}

// Stop requests for this binlogEventProducer to stop processing events as soon as possible.
func (p *binlogEventProducer) Stop() {
	p.running.Store(false)
}

// IsRunning returns true if this binlog applier is running and has not been stopped, otherwise returns false.
func (a *binlogReplicaApplier) IsRunning() bool {
	return a.running.Load()
}
