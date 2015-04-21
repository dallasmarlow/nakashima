package nakashima

import (
	"errors"
)

var (
	ErrStmtNotInsert               = errors.New(`sql statement is not of type Insert`)
	ErrRowNotValTuple              = errors.New(`sql statement row value not expected sqlparser.ValTuple type`)
	ErrRowsNotValues               = errors.New(`sql statement rows value not expected sqlparser.Values type`)
	ErrValTuplesNotMatch           = errors.New(`ValTuple entries did not match in expected range`)
	ErrMapValidationFailed         = errors.New(`ValTuple mapper validation failed due to an known failure`)
	ErrLastTupleValNotExpectedType = errors.New(`last entry in ValTuple was not expected value`)
)
