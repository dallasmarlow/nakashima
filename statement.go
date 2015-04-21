package nakashima

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"os"
	"regexp"
	"strings"

	"github.com/dallasmarlow/nakashima/sqlparser"
)

const (
	sessionStmtPattern = `^\/\*!\d+ [^\*]+\*\/`
)

type SessionStmt []byte

func isSessionStmt(stmt []byte) bool {
	if isMatch, err := regexp.Match(sessionStmtPattern, stmt); err == nil && isMatch {
		return true
	}

	return false
}

type ImportStmt struct {
	SessionStmts []SessionStmt
	InsertStmt   *sqlparser.Insert
}

func parseImportStmt(scanner *bufio.Scanner) (ImportStmt, error) {
	var buf bytes.Buffer
	var sessionStmts []SessionStmt

	for scanner.Scan() {
		// strip trailing semicolons from sql statements
		entry := bytes.TrimSuffix(scanner.Bytes(), []byte(";"))

		if isSessionStmt(entry) {
			sessionStmts = append(sessionStmts, SessionStmt(entry))
		} else {
			buf.Write(append(entry, '\n'))
		}
	}

	sqlStmt, err := sqlparser.Parse(buf.String())
	if err != nil {
		return ImportStmt{}, err
	}

	if insertStmt, ok := sqlStmt.(*sqlparser.Insert); ok {
		return ImportStmt{SessionStmts: sessionStmts, InsertStmt: insertStmt}, nil
	}

	return ImportStmt{}, ErrStmtNotInsert
}

func ParseImportStmtFromFile(filePath string) (ImportStmt, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return ImportStmt{}, err
	}
	defer file.Close()

	if strings.HasSuffix(filePath, ".gz") {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return ImportStmt{}, err
		}
		defer gzipReader.Close()

		return parseImportStmt(bufio.NewScanner(gzipReader))
	}

	return parseImportStmt(bufio.NewScanner(file))
}

func writeImportStmt(stmt ImportStmt, writer *bufio.Writer) error {
	for _, sessionStmt := range stmt.SessionStmts {
		if _, err := writer.WriteString(string(sessionStmt) + ";\n"); err != nil {
			return err
		}
	}

	if _, err := writer.WriteString(stmt.InsertStmt.String() + ";\n"); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}

func WriteImportStmtToFile(stmt ImportStmt, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var writer *bufio.Writer
	if strings.HasSuffix(filePath, ".gz") {
		gzipWriter := gzip.NewWriter(file)
		defer gzipWriter.Close()

		writer = bufio.NewWriter(gzipWriter)
	} else {
		writer = bufio.NewWriter(file)
	}

	return writeImportStmt(stmt, writer)
}
