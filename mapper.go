package nakashima

import (
	"log"
)

type StmtRowMapper interface {
	Map(src <-chan StmtValTuple) <-chan StmtValTuple
}

type StmtRowConcurrentMapper interface {
	StmtRowMapper
	NumWorkers() int
}

type StmtRowValidatedMapper interface {
	StmtRowMapper
	Validate(tA, tB StmtValTuple) (bool, error)
}

// `StmtRowMapperValidator` acts as a src and snk for a given `StmtRowValidatedMapper` while
// while using the result of that mapper's Validate function as a trigger to exit on error
type StmtRowMapperValidator struct {
	Mapper StmtRowValidatedMapper
}

func (v StmtRowMapperValidator) Map(src <-chan StmtValTuple) <-chan StmtValTuple {
	snk := make(chan StmtValTuple)

	go func() {
		mapperSrc := make(chan StmtValTuple)
		mapperSnk := startMapWorkers(v.Mapper, mapperSrc)

		defer close(mapperSrc)
		defer close(snk)

		for t := range src {
			mapperSrc <- t
			mapperRes := <-mapperSnk

			valid, err := v.Mapper.Validate(t, mapperRes)
			switch {
			case err != nil:
				log.Println(err)
				return
			case !valid:
				log.Println(ErrMapValidationFailed)
				return
			}

			snk <- mapperRes
		}
	}()

	return snk
}
