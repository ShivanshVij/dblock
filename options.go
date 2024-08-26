package dblock

import (
	"errors"
	"github.com/loopholelabs/logging/loggers/noop"
	"time"

	"github.com/loopholelabs/logging/types"
)

var (
	ErrInvalidLogger                = errors.New("invalid logger")
	ErrInvalidName                  = errors.New("invalid name")
	ErrInvalidDBType                = errors.New("invalid database type")
	ErrInvalidDatabaseURL           = errors.New("invalid database URL")
	ErrInvalidLeaseDuration         = errors.New("invalid lease duration")
	ErrInvalidLeaseRefreshFrequency = errors.New("invalid lease refresh frequency")
)

type DBType int

const (
	Undefined DBType = iota
	Postgres  DBType = iota
)

type Options struct {
	Logger                types.SubLogger
	Name                  string
	DBType                DBType
	DatabaseURL           string
	LeaseDuration         time.Duration
	LeaseRefreshFrequency time.Duration
}

func (o *Options) validate() error {
	if o.Logger == nil {
		o.Logger = noop.New(types.InfoLevel)
	}

	if o.Name == "" {
		return ErrInvalidName
	}

	if o.DBType != Postgres {
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
