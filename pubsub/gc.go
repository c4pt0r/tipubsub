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

package pubsub

import (
	"database/sql"
	"fmt"

	"github.com/c4pt0r/log"
)

const (
	// always keep recent 1000 messages
	SAFE_AMOUNT int = 1000
)

type gcWorker struct {
	streamName string
	db         *sql.DB
}

func (gc *gcWorker) getSafeOffsetID() (int64, error) {
	stmt := fmt.Sprintf(`
			SELECT MIN(t.id) 
			FROM (
				SELECT 
					id
				FROM 
					%s 
				ORDER BY
					id
				DESC LIMIT %d) as t
		`, getStreamTblName(gc.streamName), SAFE_AMOUNT)

	log.D(stmt)

	var safeOffsetID int64
	err := gc.db.QueryRow(stmt).Scan(&safeOffsetID)
	if err != nil {
		return 0, err
	}
	return safeOffsetID, nil
}

func (gc *gcWorker) deleteUntil(offsetID int64) error {
	stmt := fmt.Sprintf(`
		DELETE FROM
			%s
		WHERE
			id < ?
		LIMIT 1000
	`, getStreamTblName(gc.streamName)) // TODO: batch size

	log.D(stmt)

	for {
		res, err := gc.db.Exec(stmt, offsetID)
		if err != nil {
			return err
		}
		affectedRows, err := res.RowsAffected()
		if affectedRows == 0 {
			break
		}
	}
	return nil
}

func (gc *gcWorker) safeGC() error {
	safePoint, err := gc.getSafeOffsetID()
	if err != nil {
		return err
	}
	return gc.deleteUntil(safePoint)
}
