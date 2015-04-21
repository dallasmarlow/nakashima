package nakashima

import (
	"reflect"

	"github.com/dallasmarlow/nakashima/sqlparser"
)

type StmtRowValAppender struct {
	Val     sqlparser.ValExpr
	Workers int
}

func (m StmtRowValAppender) NumWorkers() int {
	return m.Workers
}

func (m StmtRowValAppender) Map(src <-chan StmtValTuple) <-chan StmtValTuple {
	snk := make(chan StmtValTuple)

	go func() {
		for t := range src {
			// append value to stmt row
			t.ValTuple = append(t.ValTuple, m.Val)
			snk <- t
		}

		close(snk)
	}()

	return snk
}

func (m StmtRowValAppender) Validate(tA, tB StmtValTuple) (bool, error) {
	// ensure all columns but the last col of tB are unchanged
	if reflect.DeepEqual(tA.ValTuple, tB.ValTuple[:len(tB.ValTuple)-1]) {
		// ensure last col of tB is of expected type
		if reflect.TypeOf(tB.ValTuple[len(tB.ValTuple)-1]) == reflect.TypeOf(m.Val) {
			return true, nil
		}

		return false, ErrLastTupleValNotExpectedType
	}

	return false, ErrValTuplesNotMatch
}
