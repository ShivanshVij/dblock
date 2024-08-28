// SPDX-License-Identifier: Apache-2.0

package dblock

import (
	"errors"
	"time"

	"github.com/loopholelabs/logging/loggers/noop"
	"github.com/loopholelabs/logging/types"
)

var (
	ErrInvalidOwner                 = errors.New("invalid owner")
	ErrInvalidDBType                = errors.New("invalid database type")
	ErrInvalidDatabaseURL           = errors.New("invalid database URL")
	ErrInvalidLeaseDuration         = errors.New("invalid lease duration")
	ErrInvalidLeaseRefreshFrequency = errors.New("invalid lease refresh frequency")
)

type DBType int

const (
	Undefined DBType = iota
	Postgres
	SQLite
)

type Options struct {
	Logger                types.SubLogger
	Owner                 string
	DBType                DBType
	DatabaseURL           string
	LeaseDuration         time.Duration
	LeaseRefreshFrequency time.Duration
}

func (o *Options) Validate() error {
	if o.Logger == nil {
		o.Logger = noop.New(types.InfoLevel)
	}

	if o.Owner == "" {
		return ErrInvalidOwner
	}

	if o.DBType != Postgres && o.DBType != SQLite {
		return ErrInvalidDBType
	}

	if o.DatabaseURL == "" {
		return ErrInvalidDatabaseURL
	}

	if o.LeaseDuration == 0 {
		return ErrInvalidLeaseDuration
	}

	if o.LeaseRefreshFrequency >= o.LeaseDuration {
		return ErrInvalidLeaseRefreshFrequency
	}

	if o.LeaseRefreshFrequency == 0 {
		o.LeaseRefreshFrequency = o.LeaseDuration / 2
	}

	return nil
}
