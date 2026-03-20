package dbwarp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/longbridgeapp/sqlparser"
	"gorm.io/gorm"
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("can not insert different suffix table in one query ")
	ErrInClauseCrossShard = errors.New("IN clause contains values from different shards")
)

// ResolveResult stores the parsing result
type ResolveResult struct {
	FtQuery    string // Original SQL
	StQuery    string // SQL after table sharding
	TableName  string // Table name
	DbIndex    int    // Database index, -1 means no sharding or use default
	ShardValue any    // Sharding key value
}

func (s *WarpItem) resolve(query string, args ...any) (ftQuery, stQuery, tableName string, dbIndex int, err error) {
	ftQuery = query
	stQuery = query
	dbIndex = -1 // Default -1 means no database sharding

	expr, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return ftQuery, stQuery, tableName, dbIndex, nil
	}

	var table *sqlparser.TableName
	var condition sqlparser.Expr
	var isInsert bool
	var insertNames []*sqlparser.Ident
	var insertExpressions []*sqlparser.Exprs
	var insertStmt *sqlparser.InsertStatement

	switch stmt := expr.(type) {
	case *sqlparser.SelectStatement:
		tbl, ok := stmt.FromItems.(*sqlparser.TableName)
		if !ok {
			return
		}
		if stmt.Hint != nil && stmt.Hint.Value == "nosharding" {
			return
		}
		table = tbl
		condition = stmt.Condition
	case *sqlparser.InsertStatement:
		table = stmt.TableName
		isInsert = true
		insertNames = stmt.ColumnNames
		insertExpressions = stmt.Expressions
		insertStmt = stmt
	case *sqlparser.UpdateStatement:
		condition = stmt.Condition
		table = stmt.TableName
	case *sqlparser.DeleteStatement:
		condition = stmt.Condition
		table = stmt.TableName
	default:
		return ftQuery, stQuery, "", dbIndex, sqlparser.ErrNotImplemented
	}

	tableName = table.Name.Name

	var suffix string
	var shardValue any

	if isInsert {
		var newTable *sqlparser.TableName
		for _, insertExpression := range insertExpressions {
			var value any
			var keyFind bool
			insertValues := insertExpression.Exprs
			value, _, keyFind, err = s.insertValue(s.rule.ShardingKey, insertNames, insertValues, args...)
			if err != nil {
				return
			}
			// if not find sharding key, return err
			if !keyFind {
				err = ErrMissingShardingKey
				return
			}

			// Save sharding key value for database index calculation
			if shardValue == nil {
				shardValue = value
			}

			var subSuffix string
			subSuffix, err = getSuffix(value, s.rule)
			if err != nil {
				return
			}

			// all insert value must has same suffix
			if suffix != "" && suffix != subSuffix {
				err = ErrInsertDiffSuffix
				return
			}

			suffix = subSuffix

			newTable = &sqlparser.TableName{Name: &sqlparser.Ident{Name: tableName + suffix}}
		}

		ftQuery = insertStmt.String()
		insertStmt.TableName = newTable
		stQuery = insertStmt.String()

	} else {
		var values []any
		var keyFind bool
		values, _, keyFind, err = s.nonInsertValue(s.rule.ShardingKey, condition, args...)
		if err != nil {
			return
		}
		// if not find sharding key, return err
		if !keyFind {
			err = ErrMissingShardingKey
			return
		}

		// Validate all values belong to the same shard
		if len(values) > 0 {
			shardValue = values[0]
			suffix, err = getSuffix(values[0], s.rule)
			if err != nil {
				return
			}

			// Check if all values belong to the same shard
			for _, v := range values[1:] {
				subSuffix, suffixErr := getSuffix(v, s.rule)
				if suffixErr != nil {
					err = suffixErr
					return
				}
				if subSuffix != suffix {
					err = ErrInClauseCrossShard
					return
				}
			}
		}

		newTable := &sqlparser.TableName{Name: &sqlparser.Ident{Name: tableName + suffix}}

		switch stmt := expr.(type) {
		case *sqlparser.SelectStatement:
			ftQuery = stmt.String()
			stmt.FromItems = newTable
			stmt.OrderBy = replaceOrderByTableName(stmt.OrderBy, tableName, newTable.Name.Name)
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		case *sqlparser.UpdateStatement:
			ftQuery = stmt.String()
			stmt.TableName = newTable
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		case *sqlparser.DeleteStatement:
			ftQuery = stmt.String()
			stmt.TableName = newTable
			replaceTableNameInCondition(stmt.Condition, tableName, newTable.Name.Name)
			stQuery = stmt.String()
		}
	}

	// Calculate database index
	if s.rule.ShardingDatabaseAlgorithm != nil && shardValue != nil {
		dbSuffix, dbErr := s.rule.ShardingDatabaseAlgorithm(shardValue)
		if dbErr == nil {
			dbIndex = suffixToIndex(dbSuffix)
		}
	}

	return
}

func replaceOrderByTableName(orderBy []*sqlparser.OrderingTerm, oldName, newName string) []*sqlparser.OrderingTerm {
	for i, term := range orderBy {
		if x, ok := term.X.(*sqlparser.QualifiedRef); ok {
			if x.Table.Name == oldName {
				x.Table.Name = newName
				orderBy[i].X = x
			}
		}
	}

	return orderBy
}

// replaceTableNameInCondition walks the WHERE expression tree
// and renames any qualified column references matching oldName → newName.
func replaceTableNameInCondition(expr sqlparser.Expr, oldName, newName string) {
	if expr == nil {
		return
	}

	_ = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if qr, ok := node.(*sqlparser.QualifiedRef); ok && qr.Table.Name == oldName {
			qr.Table.Name = newName
		}

		return nil
	}), expr)
}

func getSuffix(value any, r *ShardingRule) (suffix string, err error) {
	suffix, err = r.ShardingTableAlgorithm(value)
	if err != nil {
		return
	}
	return
}

func (s *WarpItem) insertValue(key string, names []*sqlparser.Ident, exprs []sqlparser.Expr, args ...any) (value any, id int64, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, 0, keyFind, errors.New("column names and expressions mismatch")
	}

	for i, name := range names {
		if name.Name == key {
			switch expr := exprs[i].(type) {
			case *sqlparser.BindExpr:
				value = args[expr.Pos]
			case *sqlparser.StringLit:
				value = expr.Value
			case *sqlparser.NumberLit:
				value = expr.Value
			default:
				return nil, 0, keyFind, sqlparser.ErrNotImplemented
			}
			keyFind = true
			break
		}
	}
	if !keyFind {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

func (s *WarpItem) nonInsertValue(key string, condition sqlparser.Expr, args ...any) (values []any, id int64, keyFind bool, err error) {
	err = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if n, ok := node.(*sqlparser.BinaryExpr); ok {
			x, ok := n.X.(*sqlparser.Ident)
			if !ok {
				if q, ok2 := n.X.(*sqlparser.QualifiedRef); ok2 {
					x = q.Column
					ok = true
				}
			}
			if ok {
				// Handle = operator
				if x.Name == key && n.Op == sqlparser.EQ {
					keyFind = true
					var val any
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						val = args[expr.Pos]
					case *sqlparser.StringLit:
						val = expr.Value
					case *sqlparser.NumberLit:
						val = expr.Value
					default:
						return sqlparser.ErrNotImplemented
					}
					values = []any{val}
					return nil
				} else if x.Name == "id" && n.Op == sqlparser.EQ {
					// Handle id = ? for backward compatibility
					switch expr := n.Y.(type) {
					case *sqlparser.BindExpr:
						v := args[expr.Pos]
						var ok bool
						if id, ok = v.(int64); !ok {
							return fmt.Errorf("ID should be int64 type")
						}
					case *sqlparser.NumberLit:
						id, err = strconv.ParseInt(expr.Value, 10, 64)
						if err != nil {
							return err
						}
					default:
						return ErrInvalidID
					}
					return nil
				} else if x.Name == key && n.Op == sqlparser.IN {
					// Handle IN operator
					keyFind = true
					exprs, ok := n.Y.(*sqlparser.Exprs)
					if !ok {
						return fmt.Errorf("IN clause must contain a list of values")
					}

					for _, expr := range exprs.Exprs {
						var val any
						switch e := expr.(type) {
						case *sqlparser.BindExpr:
							val = args[e.Pos]
						case *sqlparser.StringLit:
							val = e.Value
						case *sqlparser.NumberLit:
							val = e.Value
						default:
							return fmt.Errorf("unsupported expression type in IN clause: %T", e)
						}
						values = append(values, val)
					}
					return nil
				}
			}
		}
		return nil
	}), condition)
	if err != nil {
		return
	}

	if !keyFind && id == 0 {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}

// suffixToIndex converts suffix string to index, e.g. "_0" -> 0, "_1" -> 1
func suffixToIndex(suffix string) int {
	if suffix == "" {
		return 0
	}
	// Remove leading underscore
	s := strings.TrimPrefix(suffix, "_")
	idx, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return idx
}

// getTargetPool returns the target connection pool based on database index
func (s *WarpItem) getTargetPool(dbIndex int, op Operation, stmt *gorm.Statement) gorm.ConnPool {
	// If there are multiple sharding resolvers and index is valid
	if len(s.resolvers) > 0 && dbIndex >= 0 && dbIndex < len(s.resolvers) {
		return s.resolvers[dbIndex].resolve(stmt, op)
	}

	return nil
}
