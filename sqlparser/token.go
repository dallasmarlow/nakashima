// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"fmt"
	"strings"
)

const eofChar = 0x100

// Tokenizer is the struct used to generate SQL tokens for the parser.
type Tokenizer struct {
	InStream      *strings.Reader
	AllowComments bool
	ForceEOF      bool
	lastChar      uint16
	Position      int
	errorToken    []byte
	LastError     string
	posVarIndex   int
	ParseTree     Statement
}

// NewStringTokenizer creates a new Tokenizer for the sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	return &Tokenizer{InStream: strings.NewReader(sql)}
}

var keywords = map[string]int{
	"SELECT": tokSelect,
	"INSERT": tokInsert,
	"UPDATE": tokUpdate,
	"DELETE": tokDelete,
	"FROM":   tokFrom,
	"WHERE":  tokWhere,
	"GROUP":  tokGroup,
	"HAVING": tokHaving,
	"ORDER":  tokOrder,
	"BY":     tokBy,
	"LIMIT":  tokLimit,
	"OFFSET": tokOffset,
	"FOR":    tokFor,

	"UNION":     tokUnion,
	"ALL":       tokAll,
	"MINUS":     tokMinus,
	"EXCEPT":    tokExcept,
	"INTERSECT": tokIntersect,

	"JOIN":          tokJoin,
	"STRAIGHT_JOIN": tokStraightJoin,
	"LEFT":          tokLeft,
	"RIGHT":         tokRight,
	"INNER":         tokInner,
	"OUTER":         tokOuter,
	"CROSS":         tokCross,
	"NATURAL":       tokNatural,
	"USE":           tokUse,
	"FORCE":         tokForce,
	"ON":            tokOn,
	"INTO":          tokInto,

	"DISTINCT":  tokDistinct,
	"CASE":      tokCase,
	"WHEN":      tokWhen,
	"THEN":      tokThen,
	"ELSE":      tokElse,
	"END":       tokEnd,
	"AS":        tokAs,
	"AND":       tokAnd,
	"OR":        tokOr,
	"NOT":       tokNot,
	"EXISTS":    tokExists,
	"IN":        tokIn,
	"IS":        tokIs,
	"LIKE":      tokLike,
	"BETWEEN":   tokBetween,
	"NULL":      tokNull,
	"ASC":       tokAsc,
	"DESC":      tokDesc,
	"VALUES":    tokValues,
	"DUPLICATE": tokDuplicate,
	"KEY":       tokKey,
	"DEFAULT":   tokDefault,
	"SET":       tokSet,
	"LOCK":      tokLock,

	"CREATE":   tokCreate,
	"ALTER":    tokAlter,
	"RENAME":   tokRename,
	"DROP":     tokDrop,
	"TRUNCATE": tokTruncate,
	"SHOW":     tokShow,
	"TABLE":    tokTable,
	"TABLES":   tokTables,
	"DATABASE": tokDatabase,
	"INDEX":    tokIndex,
	"VIEW":     tokView,
	"COLUMNS":  tokColumns,
	"FULL":     tokFull,
	"TO":       tokTo,
	"IGNORE":   tokIgnore,
	"IF":       tokIf,
	"UNIQUE":   tokUnique,
	"USING":    tokUsing,
}

// Lex returns the next token form the Tokenizer.
// This function is used by go yacc.
func (tkn *Tokenizer) Lex(lval *yySymType) int {
	typ, val := tkn.Scan()
	for typ == tokComment {
		if tkn.AllowComments {
			break
		}
		typ, val = tkn.Scan()
	}
	switch typ {
	case tokID, tokString, tokNumber, tokValueArg, tokComment, tokAnd, tokOr, tokNot:
		lval.str = string(val)
	}
	tkn.errorToken = val
	return typ
}

// Error is called by go yacc if there's a parsing error.
func (tkn *Tokenizer) Error(err string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	if tkn.errorToken != nil {
		fmt.Fprintf(buf, "%s at position %v near %s", err, tkn.Position, tkn.errorToken)
	} else {
		fmt.Fprintf(buf, "%s at position %v", err, tkn.Position)
	}
	tkn.LastError = buf.String()
}

// Scan scans the tokenizer for the next token and returns
// the token type and an optional value.
func (tkn *Tokenizer) Scan() (int, []byte) {
	if tkn.ForceEOF {
		return 0, nil
	}

	if tkn.lastChar == 0 {
		tkn.next()
	}
	tkn.skipBlank()
	switch ch := tkn.lastChar; {
	case isLetter(ch):
		return tkn.scanIdentifier()
	case isDigit(ch):
		return tkn.scanNumber(false)
	case ch == ':':
		return tkn.scanBindVar()
	default:
		tkn.next()
		switch ch {
		case eofChar:
			return 0, nil
		case '&':
			if tkn.lastChar == '&' {
				tkn.next()
				return tokAnd, []byte("&&")
			}
			return int(ch), nil
		case '|':
			if tkn.lastChar == '|' {
				tkn.next()
				return tokOr, []byte("||")
			}
			return int(ch), nil
		case '=', ',', ';', '(', ')', '+', '*', '%', '^', '~':
			return int(ch), nil
		case '?':
			tkn.posVarIndex++
			buf := new(bytes.Buffer)
			fmt.Fprintf(buf, ":v%d", tkn.posVarIndex)
			return tokValueArg, buf.Bytes()
		case '.':
			if isDigit(tkn.lastChar) {
				return tkn.scanNumber(true)
			}
			return int(ch), nil
		case '/':
			switch tkn.lastChar {
			case '/':
				tkn.next()
				return tkn.scanCommentType1("//")
			case '*':
				tkn.next()
				return tkn.scanCommentType2()
			default:
				return int(ch), nil
			}
		case '-':
			if tkn.lastChar == '-' {
				tkn.next()
				return tkn.scanCommentType1("--")
			}
			return int(ch), nil
		case '<':
			switch tkn.lastChar {
			case '>':
				tkn.next()
				return tokNE, nil
			case '=':
				tkn.next()
				switch tkn.lastChar {
				case '>':
					tkn.next()
					return tokNullSafeEqual, nil
				default:
					return tokLE, nil
				}
			default:
				return int(ch), nil
			}
		case '>':
			if tkn.lastChar == '=' {
				tkn.next()
				return tokGE, nil
			}
			return int(ch), nil
		case '!':
			if tkn.lastChar == '=' {
				tkn.next()
				return tokNE, nil
			}
			return tokNot, []byte("!")
		case '\'', '"':
			return tkn.scanString(ch, tokString)
		case '`':
			return tkn.scanString(ch, tokID)
		default:
			return tokLexError, []byte{byte(ch)}
		}
	}
}

func (tkn *Tokenizer) skipBlank() {
	ch := tkn.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.next()
		ch = tkn.lastChar
	}
}

func (tkn *Tokenizer) scanIdentifier() (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(tkn.lastChar))
	for tkn.next(); isLetter(tkn.lastChar) || isDigit(tkn.lastChar); tkn.next() {
		buffer.WriteByte(byte(tkn.lastChar))
	}
	uppered := bytes.ToUpper(buffer.Bytes())
	if keywordID, found := keywords[string(uppered)]; found {
		return keywordID, uppered
	}
	return tokID, buffer.Bytes()
}

func (tkn *Tokenizer) scanBindVar() (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(tkn.lastChar))
	for tkn.next(); isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || tkn.lastChar == '.'; tkn.next() {
		buffer.WriteByte(byte(tkn.lastChar))
	}
	if buffer.Len() == 1 {
		return tokLexError, buffer.Bytes()
	}
	return tokValueArg, buffer.Bytes()
}

func (tkn *Tokenizer) scanMantissa(base int, buffer *bytes.Buffer) {
	for digitVal(tkn.lastChar) < base {
		tkn.consumeNext(buffer)
	}
}

func (tkn *Tokenizer) scanNumber(seenDecimalPoint bool) (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	if seenDecimalPoint {
		buffer.WriteByte('.')
		tkn.scanMantissa(10, buffer)
		goto exponent
	}

	if tkn.lastChar == '0' {
		// int or float
		tkn.consumeNext(buffer)
		if tkn.lastChar == 'x' || tkn.lastChar == 'X' {
			// hexadecimal int
			tkn.consumeNext(buffer)
			tkn.scanMantissa(16, buffer)
		} else {
			// octal int or float
			seenDecimalDigit := false
			tkn.scanMantissa(8, buffer)
			if tkn.lastChar == '8' || tkn.lastChar == '9' {
				// illegal octal int or float
				seenDecimalDigit = true
				tkn.scanMantissa(10, buffer)
			}
			if tkn.lastChar == '.' || tkn.lastChar == 'e' || tkn.lastChar == 'E' {
				goto fraction
			}
			// octal int
			if seenDecimalDigit {
				return tokLexError, buffer.Bytes()
			}
		}
		goto exit
	}

	// decimal int or float
	tkn.scanMantissa(10, buffer)

fraction:
	if tkn.lastChar == '.' {
		tkn.consumeNext(buffer)
		tkn.scanMantissa(10, buffer)
	}

exponent:
	if tkn.lastChar == 'e' || tkn.lastChar == 'E' {
		tkn.consumeNext(buffer)
		if tkn.lastChar == '+' || tkn.lastChar == '-' {
			tkn.consumeNext(buffer)
		}
		tkn.scanMantissa(10, buffer)
	}

exit:
	return tokNumber, buffer.Bytes()
}

func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	for {
		ch := tkn.lastChar
		tkn.next()
		if ch == delim {
			if tkn.lastChar == delim {
				tkn.next()
			} else {
				break
			}
		} else if ch == '\\' {
			if tkn.lastChar == eofChar {
				return tokLexError, buffer.Bytes()
			}
			if decodedChar := decodeMap[byte(tkn.lastChar)]; decodedChar == dontEscape {
				ch = tkn.lastChar
			} else {
				ch = uint16(decodedChar)
			}
			tkn.next()
		}
		if ch == eofChar {
			return tokLexError, buffer.Bytes()
		}
		buffer.WriteByte(byte(ch))
	}
	return typ, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType1(prefix string) (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString(prefix)
	for tkn.lastChar != eofChar {
		if tkn.lastChar == '\n' {
			tkn.consumeNext(buffer)
			break
		}
		tkn.consumeNext(buffer)
	}
	return tokComment, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType2() (int, []byte) {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString("/*")
	for {
		if tkn.lastChar == '*' {
			tkn.consumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.consumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == eofChar {
			return tokLexError, buffer.Bytes()
		}
		tkn.consumeNext(buffer)
	}
	return tokComment, buffer.Bytes()
}

func (tkn *Tokenizer) consumeNext(buffer *bytes.Buffer) {
	if tkn.lastChar == eofChar {
		// This should never happen.
		panic("unexpected EOF")
	}
	buffer.WriteByte(byte(tkn.lastChar))
	tkn.next()
}

func (tkn *Tokenizer) next() {
	if ch, err := tkn.InStream.ReadByte(); err != nil {
		// Only EOF is possible.
		tkn.lastChar = eofChar
	} else {
		tkn.lastChar = uint16(ch)
	}
	tkn.Position++
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '@'
}

func digitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}
