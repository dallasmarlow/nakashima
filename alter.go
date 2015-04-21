package nakashima

import (
	"database/sql"
	"log"
	"time"

	"github.com/dallasmarlow/nakashima/mydumper"
	"github.com/dallasmarlow/nakashima/mysql"
)

var (
	DefaultNumThreads    = 64
	DefaultStmtSize      = 1000000
	DefaultRowRangeSize  = 1000000
	DefaultExportTimeout = 8 * time.Hour
	DefaultImportTimeout = 16 * time.Hour
)

type ExportAlterStage int

const (
	ExportAlterStageUnknown ExportAlterStage = iota
	ExportAlterStageInit
	ExportAlterStageExport
	ExportAlterStageRewrite
	ExportAlterStageImport
	ExportAlterStageFinalize
)

type ExportAlter struct {
	conn                                                                                       *sql.DB
	stage                                                                                      ExportAlterStage
	MyDumperExportRoot, MyDumperExecPath, Host, Username, Password, Database, Table, AlterStmt string
	Port, NumThreads, StmtSize, RowRangeSize                                                   int
}

func NewExportAlter(exportRoot, myDumperExecPath, host, username, password, database, table, alterStmt string, port int) (*ExportAlter, error) {
	log.Println("Init new export alter for:", database, table)

	conn, err := mysql.NewConn(database, username, password, host, port)
	if err != nil {
		return nil, err
	}

	return &ExportAlter{
		conn,
		ExportAlterStageInit,
		exportRoot,
		myDumperExecPath,
		host,
		username,
		password,
		database,
		table,
		alterStmt,
		port,
		DefaultNumThreads,
		DefaultStmtSize,
		DefaultRowRangeSize,
	}, nil
}

func (a *ExportAlter) StartExport(setReadOnly, stopSlave bool) error {
	a.stage = ExportAlterStageExport

	if setReadOnly {
		log.Println(`Enabling mysql read_only setting`)
		if err := mysql.SetReadOnly(a.conn); err != nil {
			return err
		}
	}

	if stopSlave {
		log.Println(`Pausing mysql replication`)
		if err := mysql.StopSlave(a.conn); err != nil {
			return err
		}
	}

	dumper := mydumper.New(
		a.MyDumperExecPath,
		a.MyDumperExportRoot,
		a.Username,
		a.Password,
		a.Database,
		a.Table,
		a.NumThreads,
	)

	dumper.Compress = true
	dumper.NoSchemas = true
	dumper.StmtSize = a.StmtSize
	dumper.RowRangeSize = a.RowRangeSize

	log.Println(`Starting mysql table export`)
	stdout, stderr, err := dumper.Exec(DefaultExportTimeout)
	if stdout != `` {
		log.Println(`export stdout:`, stdout)
	}
	if stderr != `` {
		log.Println(`export stderr:`, stderr)
	}

	return err
}
