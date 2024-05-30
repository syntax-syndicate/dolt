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

package binlogreplication

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"
)

// binlogStreamer is responsible for receiving binlog events over its eventChan
// channel, and streaming those out to a connected replica over a MySQL connection.
// It also sends heartbeat events to the replica over the same connection at
// regular intervals. There is one streamer per connected replica.
type binlogStreamer struct {
	quitChan  chan struct{}
	eventChan chan []mysql.BinlogEvent
	ticker    *time.Ticker
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan:  make(chan struct{}),
		eventChan: make(chan []mysql.BinlogEvent, 5),
		ticker:    time.NewTicker(30 * time.Second),
	}
}

// startStream listens for new binlog events sent to this streamer over its binlog event
// channel and sends them over |conn|. It also listens for ticker ticks to send hearbeats
// over |conn|. The specified |binlogFormat| is used to define the format of binlog events
// and |binlogStream| records the position of the stream. This method blocks until an error
// is received over the stream (e.g. the connection closing) or the streamer is closed,
// through it's quit channel.
func (streamer *binlogStreamer) startStream(_ *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogStream *mysql.BinlogStream, logfile string) error {
	logrus.Errorf("Starting stream... (connection ID: %d)", conn.ConnectionID)

	// TODO: Maybe we should just ask the LogManager to give us the file for reading?
	file, err := os.Open(logfile)
	if err != nil {
		return err
	}
	buffer := make([]byte, len(binlogFileMagicNumber))
	bytesRead, err := file.Read(buffer)
	if err != nil {
		return err
	}
	if bytesRead != len(binlogFileMagicNumber) || string(buffer) != string(binlogFileMagicNumber) {
		return fmt.Errorf("invalid magic number in binlog file!")
	}

	defer file.Close()

	for {
		logrus.Trace("binlog streamer is listening for messages")

		select {
		case <-streamer.quitChan:
			logrus.Trace("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			logrus.Trace("sending binlog heartbeat")
			if err := sendHeartbeat(conn, binlogFormat, binlogStream); err != nil {
				return err
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}

		case events := <-streamer.eventChan:
			logrus.Tracef("streaming %d binlog events", len(events))
			for _, event := range events {
				if err := conn.WriteBinlogEvent(event, false); err != nil {
					return err
				}
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush binlog connection: %s", err.Error())
			}

		default:
			// TODO: Start with a simple polling approach, but we may need to change to
			//       inotify or something more efficient in the future.
			logrus.Debug("checking file for new data...")
			eof := false
			for !eof {
				headerBuffer := make([]byte, 4+1+4+4+4+2)
				bytesRead, err := file.Read(headerBuffer)
				if err != nil && err != io.EOF {
					return err
				}
				if err == io.EOF {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				// Event Header:
				//timestamp := headerBuffer[0:4]
				//eventType := headerBuffer[4]
				//serverId := binary.LittleEndian.Uint32(headerBuffer[5:5+4])
				eventSize := binary.LittleEndian.Uint32(headerBuffer[9 : 9+4])

				payloadBuffer := make([]byte, eventSize-uint32(len(headerBuffer)))
				bytesRead, err = file.Read(payloadBuffer)
				if err != nil && err != io.EOF {
					return err
				}
				if err == io.EOF {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				logrus.Errorf("read %d bytes from binlog file", bytesRead)

				if bytesRead > 0 {
					// TODO: We can't use conn.Conn.Write!
					//       We need to use conn.WriteBinlogEvent, because it writes other data to the socket (e.g. OK packets)

					binlogEvent := mysql.NewMysql56BinlogEvent(append(headerBuffer, payloadBuffer...))

					if binlogEvent.IsRotate() {
						// TODO: if this is a rotate event, then we need to switch to the new binlog file after
						//       we write this event out to the replica.
						binlogEvent.IsRotate()
						newLogfile := payloadBuffer[8:]
						logrus.Errorf("Rotatating to new binlog file: %s", newLogfile)
						// TODO: We need a way to convert the bare filename to a full path, in the right directory
					}

					// Components of Log File support:
					//  - streamers streaming from log files
					//  - rotating log files (on startup, and on flush logs, and on size threshold)
					//  - purging log files (on request, and automatically, aging out)
					//  - looking up starting point in log file, based on GTID
					// TOTAL TIME ESTIMATE: 15 days?

					err := conn.WriteBinlogEvent(binlogEvent, false)
					if err != nil {
						return err
					}

					err = conn.FlushBuffer()
					if err != nil {
						return err
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// binlogStreamerManager manages a collection of binlogStreamers, one for reach connected replica,
// and implements the doltdb.DatabaseUpdateListener interface to receive notifications of database
// changes that need to be turned into binlog events and then sent to connected replicas.
type binlogStreamerManager struct {
	streamers      []*binlogStreamer
	streamersMutex sync.Mutex
	quitChan       chan struct{}
	logManager     *LogManager
}

// NewBinlogStreamerManager creates a new binlogStreamerManager instance.
func newBinlogStreamerManager() *binlogStreamerManager {
	manager := &binlogStreamerManager{
		streamers:      make([]*binlogStreamer, 0),
		streamersMutex: sync.Mutex{},
		quitChan:       make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-manager.quitChan:
				streamers := manager.copyStreamers()
				for _, streamer := range streamers {
					streamer.quitChan <- struct{}{}
				}
				return
			}
		}
	}()

	return manager
}

// copyStreamers returns a copy of the streamers owned by this streamer manager.
func (m *binlogStreamerManager) copyStreamers() []*binlogStreamer {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	results := make([]*binlogStreamer, len(m.streamers))
	copy(results, m.streamers)
	return results
}

// StartStream starts a new binlogStreamer and streams events over |conn| until the connection
// is closed, the streamer is sent a quit signal over its quit channel, or the streamer receives
// errors while sending events over the connection. Note that this method blocks until the
// streamer exits.
func (m *binlogStreamerManager) StartStream(ctx *sql.Context, conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	streamer := newBinlogStreamer()
	m.addStreamer(streamer)
	defer m.removeStreamer(streamer)

	return streamer.startStream(ctx, conn, binlogFormat, binlogStream, m.logManager.currentBinlogFilepath())
}

// addStreamer adds |streamer| to the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) addStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = append(m.streamers, streamer)
}

// removeStreamer removes |streamer| from the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) removeStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = make([]*binlogStreamer, len(m.streamers)-1, 0)
	for _, element := range m.streamers {
		if element != streamer {
			m.streamers = append(m.streamers, element)
		}
	}
}

// LogManager sets the LogManager this streamer manager will work with to find
// and read from binlog files.
func (m *binlogStreamerManager) LogManager(manager *LogManager) {
	m.logManager = manager
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat *mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogStream.Timestamp = uint32(0) // Timestamp is zero for a heartbeat event
	logrus.WithField("log_position", binlogStream.LogPosition).Tracef("sending heartbeat")

	binlogEvent := mysql.NewHeartbeatEvent(*binlogFormat, binlogStream)
	return conn.WriteBinlogEvent(binlogEvent, false)
}
