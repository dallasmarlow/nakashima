package nakashima

import (
	"github.com/dallasmarlow/nakashima/sqlparser"
)

type StmtValTuple struct {
	Key      int
	ValTuple sqlparser.ValTuple
}

type StmtRowProcessor struct {
	src chan<- StmtValTuple
	snk <-chan StmtValTuple
}

func NewStmtRowProcessor(mappers ...StmtRowMapper) *StmtRowProcessor {
	src := make(chan StmtValTuple)
	var snk <-chan StmtValTuple

	for _, mapper := range mappers {
		if snk == nil {
			snk = startMapWorkers(mapper, src)
		} else {
			snk = startMapWorkers(mapper, snk)
		}
	}

	return &StmtRowProcessor{src, snk}
}

func (p *StmtRowProcessor) Enqueue(t StmtValTuple) {
	p.src <- t
}

func (p *StmtRowProcessor) EnqueueValuesAndClose(vals sqlparser.Values) error {
	defer p.Close()

	for i, val := range vals {
		if t, ok := val.(sqlparser.ValTuple); ok {
			p.Enqueue(StmtValTuple{i, t})
		} else {
			return ErrRowNotValTuple
		}
	}

	return nil
}

func (p *StmtRowProcessor) Close() {
	close(p.src)
}

func (p *StmtRowProcessor) Reduce(f func(StmtValTuple)) {
	for t := range p.snk {
		f(t)
	}
}
