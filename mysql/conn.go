package mysql

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const (
	localDSN = `root:xxxx@tcp(localhost:3306)/xxxx`
)

func newDSN(database, username, password, host string, port int) string {
	return fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s`, username, password, host, port, database)
}

func NewConn(database, username, password, host string, port int) (*sql.DB, error) {
	return sql.Open(`mysql`, newDSN(database, username, password, host, port))
}

func SetReadOnly(conn *sql.DB) error {
	_, err := conn.Exec(`set global read_only = 1`)

	return err
}

func DisableReadOnly(conn *sql.DB) error {
	_, err := conn.Exec(`set global read_only = 0`)

	return err
}

func StopSlave(conn *sql.DB) error {
	_, err := conn.Exec(`stop slave`)

	return err
}

func StartSlave(conn *sql.DB) error {
	_, err := conn.Exec(`start slave`)

	return err
}
