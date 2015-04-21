package nakashima

import (
	"sync"
)

func startMapWorkers(mapper StmtRowMapper, src <-chan StmtValTuple) <-chan StmtValTuple {
	if concMapper, ok := mapper.(StmtRowConcurrentMapper); ok {
		workerChs := make([]<-chan StmtValTuple, concMapper.NumWorkers())
		for i := 0; i < len(workerChs); i++ {
			workerChs[i] = concMapper.Map(src)
		}

		return stmtRowMergeCh(workerChs...)
	} else {
		return mapper.Map(src)
	}
}

func stmtRowMergeCh(chs ...<-chan StmtValTuple) <-chan StmtValTuple {
	var wg sync.WaitGroup
	snk := make(chan StmtValTuple)

	go func() {
		for _, ch := range chs {
			wg.Add(1)

			go func(ch <-chan StmtValTuple) {
				for t := range ch {
					snk <- t
				}

				wg.Done()
			}(ch)
		}

		wg.Wait()
		close(snk)
	}()

	return snk
}
