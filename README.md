# nakashima
![](http://toddmerrillstudio.com/wp-content/uploads/2013/08/Nakashima-Portrait.jpg)
```go
package sourcetype
 
import (
	"log"
 
	naka "github.com/dallasmarlow/nakashima"
	"github.com/dallasmarlow/nakashima/sqlparser"
)
 
func Run(importStmt naka.ImportStmt) (naka.ImportStmt, error) {
	log.Println("Initializing alter process for table: ", importStmt.InsertStmt.Table)
 
	if stmtRows, ok := importStmt.InsertStmt.Rows.(sqlparser.Values); ok {
		// setup job
		processor := naka.NewStmtRowProcessor(
			naka.StmtRowMapperValidator{
				Mapper: naka.StmtRowValAppender{
					Val:     &sqlparser.NullVal{},
					Workers: 5,
				},
			},
		)
 
		// enqueue stmt rows
		go func() {
			if err := processor.EnqueueValuesAndClose(stmtRows); err != nil {
				log.Fatal(err)
			}
		}()
 
		// process results
		res := make(sqlparser.Values, len(stmtRows))
		processor.Reduce(func(t naka.StmtValTuple) {
			res[t.Key] = t.ValTuple
		})
 
		importStmt.InsertStmt.Rows = res
		return importStmt, nil
	} else {
		return naka.ImportStmt{}, naka.ErrRowsNotValues
	}
}
```
