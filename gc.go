// Copyright 2022 Ed Huang<i@huangdx.net>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tipubsub

import (
	"database/sql"
	"fmt"

	"github.com/c4pt0r/log"
)

type gcWorker struct {
	db  *sql.DB
	cfg *Config
}

func newGCWorker(db *sql.DB, config *Config) *gcWorker {
	return &gcWorker{
		db:  db,
		cfg: config,
	}
}

// getSafeOffsetID returns the offsetID of the last message in the stream
func (gc *gcWorker) getSafeOffsetID(streamName string) (int64, error) {
	stmt := fmt.Sprintf(`
			SELECT MIN(t.id) 
			FROM (
				SELECT 
					id
				FROM 
					%s 
				ORDER BY
					id
				DESC LIMIT %d
			) as t
		`, getStreamTblName(streamName), gc.cfg.GCKeepItems)

	var safeOffsetID int64
	err := gc.db.QueryRow(stmt).Scan(&safeOffsetID)
	if err != nil {
		return 0, err
	}
	return safeOffsetID, nil
}

// deleteUntil deletes all messages in the stream before the given offsetID
func (gc *gcWorker) deleteUntil(streamName string, offsetID int64) error {
	stmt := fmt.Sprintf(`
		DELETE FROM
			%s
		WHERE
			id < ?
		LIMIT %d
	`, getStreamTblName(streamName), gc.cfg.MaxBatchSize) // TODO: batch size
	for {
		res, err := gc.db.Exec(stmt, offsetID)
		if err != nil {
			return err
		}
		affectedRows, err := res.RowsAffected()
		if affectedRows == 0 {
			break
		}
		log.D("GC", "Deleted %d messages", affectedRows)
	}
	return nil
}

// safeGC deletes all messages in the stream before the last SAFE_AMOUNT messages
func (gc *gcWorker) safeGC(streamName string) error {
	safePoint, err := gc.getSafeOffsetID(streamName)
	if err != nil {
		return err
	}
	return gc.deleteUntil(streamName, safePoint)
}
