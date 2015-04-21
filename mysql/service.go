package mysql

import (
	"errors"
	"strings"
	"time"

	"github.com/dallasmarlow/nakashima/bash"
)

var (
	ErrStatusUnknown  = errors.New(`Unknown response from command 'service mysql status'`)
	ErrStderrNotEmpty = errors.New(`Expected stderr from mysql service command to be empty`)
)

type StatusVal int

const (
	StatusUnknown StatusVal = iota
	StatusStopped
	StatusRunning
)

func (s StatusVal) String() string {
	switch s {
	case StatusStopped:
		return `Status: stopped`
	case StatusRunning:
		return `Status: running`
	}

	return `Status: unknown`
}

type ServiceStatus struct {
	Status    StatusVal
	CheckedAt time.Time
}

func (s ServiceStatus) IsRunning() bool {
	return s.Status == StatusRunning
}

func ServiceStart() (ServiceStatus, error) {
	return serviceAction(`start`)
}

func ServiceStop() (ServiceStatus, error) {
	return serviceAction(`stop`)
}

func ServiceGetStatus() (ServiceStatus, error) {
	return serviceAction(`status`)
}

func serviceAction(action string) (ServiceStatus, error) {
	stdout, stderr, err := bash.Exec(`service mysql ` + action)

	switch {
	case err != nil:
		return ServiceStatus{}, err
	case stderr.String() != ``:
		return ServiceStatus{}, ErrStderrNotEmpty
	}

	return parseServiceStatusMsg(stdout.String())
}

func parseServiceStatusMsg(msg string) (ServiceStatus, error) {
	switch {
	case strings.HasPrefix(msg, `mysql start/running`):
		return ServiceStatus{StatusRunning, time.Now()}, nil
	case strings.HasPrefix(msg, `mysql stop/waiting`):
		return ServiceStatus{StatusStopped, time.Now()}, nil
	}

	return ServiceStatus{}, ErrStatusUnknown
}
