// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"database/sql"
	"time"
)

func Wait(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func RowScan(rows *sql.Rows, dest ...any) error {
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := rows.Scan(dest...)
	if err != nil {
		return err
	}
	return rows.Close()
}
