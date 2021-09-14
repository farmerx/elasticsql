package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// handleComparisonExpr ...
// 处理表达式
// step 0
func handleComparisonExpr(comparisonExpr *sqlparser.ComparisonExpr, topLevel bool) (dslExpr string, err error) {
	var colName, colVal = ``, ``
	if colName, err = getComparisonExprLeftVal(comparisonExpr); err != nil {
		return ``, err
	}
	if colVal, err = getComparisonExprRightVal(comparisonExpr); err != nil {
		return ``, err
	}
	dslExpr = handlecomparisonExprOperator(comparisonExpr, colVal, colName)
	// the root node need to have bool and must
	if topLevel {
		dslExpr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, dslExpr)
	}
	return dslExpr, nil
}

// getComparisonExprLeftVal ...
// 用来获取表达式的行名
// step 1
func getComparisonExprLeftVal(comparisonExpr *sqlparser.ComparisonExpr) (leftVal string, err error) {
	// 获取 where a=‘hahaha’ 获取a这个字段
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("ElasticSQL: invalid comparison expression, the left must be a column name")
	}
	return strings.Replace(sqlparser.String(colName), "`", "", -1), nil
}

// getComparisonExprRight ...
// 用来获取表达式的行值
// step 2
func getComparisonExprRightVal(comparisonExpr *sqlparser.ComparisonExpr) (rightStr string, err error) {
	switch sqlVal := comparisonExpr.Right.(type) {
	case *sqlparser.SQLVal:
		replace := strings.NewReplacer("`", "")
		rightStr = replace.Replace(string(sqlVal.Val))
	case *sqlparser.FuncExpr:
		// parse nested
		if rightStr, err = buildNestedFuncStrValue(sqlVal); err != nil {
			return "", err
		}
	case *sqlparser.ColName:
		return "", errors.New("ElasticSQL: column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(comparisonExpr.Right)
	default:
		// cannot reach here
	}
	return rightStr, nil
}

// handlecomparisonExprOperator ...
// 用来获取表达式的关系
// step 3
func handlecomparisonExprOperator(comparisonExpr *sqlparser.ComparisonExpr, colVal, colName string) (dslExpr string) {
	switch comparisonExpr.Operator {
	case ">=":
		dslExpr = fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v"}}}`, colName, colVal)
	case "<=":
		dslExpr = fmt.Sprintf(`{"range" : {"%v" : {"to" : "%v"}}}`, colName, colVal)
	case "=":
		// query_string 全文检索，不限制字段
		if strings.ToLower(colName) == `query_string` {
			dslExpr = fmt.Sprintf(`{"query_string" : {"query" : "%v" }}`, colVal)
		} else {
			dslExpr = fmt.Sprintf(`{"term" : {"%v" : "%v"}}`, colName, colVal)
		}
		// else if strings.LastIndex(colName, "_match") > 0 {
		// 	dslExpr = fmt.Sprintf(`{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}`, colName, colVal)
		// }
	case ">":
		dslExpr = fmt.Sprintf(`{"range" : {"%v" : {"gt" : "%v"}}}`, colName, colVal)
	case "<":
		dslExpr = fmt.Sprintf(`{"range" : {"%v" : {"lt" : "%v"}}}`, colName, colVal)
	case "!=":
		if strings.ToLower(colName) == `query_string` {
			dslExpr = fmt.Sprintf(`{"bool" : {"must_not" : [{"query_string" : {"query" : "%v" }}]}}`, colVal)
		} else {
			dslExpr = fmt.Sprintf(`{"bool" : {"must_not" : [{"term" : {"%v" : "%v"}}]}}`, colName, colVal)
			//dslExpr = fmt.Sprintf(`{"bool" : {"must_not" : [{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}]}}`, colName, colVal)
		}
	case "in":
		replace := strings.NewReplacer(`'`, `"`, `(`, ``, `)`, ``) // the default valTuple is ('1', '2', '3') like
		dslExpr = fmt.Sprintf(`{"terms" : {"%v" : [%v]}}`, colName, replace.Replace(colVal))
	case "not in":
		replace := strings.NewReplacer(`'`, `"`, `(`, ``, `)`, ``)
		dslExpr = fmt.Sprintf(`{"bool" : {"must_not" : {"terms" : {"%v" : [%v]}}}}`, colName, replace.Replace(colVal))
	case "like":
		// 不是所有的字段都能进行like操作的，比如说整形等等
		colVal = strings.Replace(colVal, `%`, `*`, -1)
		dslExpr = fmt.Sprintf(`{"wildcard" : {"%v" : "%v"}}`, colName, colVal)
	case "not like":
		colVal = strings.Replace(colVal, `%`, `*`, -1)
		dslExpr = fmt.Sprintf(`{"bool" : {"must_not" : {"wildcard" : {"%v" : "%v"}}}}`, colName, colVal)
	default:
	}
	return dslExpr
}

//handleIsExpr build is null,is not null, is true, is false condition
func handleIsExpr(comparisonExpr *sqlparser.IsExpr) (dslExpr string, err error) {

	colNameParser, ok := comparisonExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("ElasticSQL: invalid comparison expression, the left must be a column name")
	}
	colName := strings.Replace(sqlparser.String(colNameParser), "`", "", -1)
	switch comparisonExpr.Operator {
	case sqlparser.IsNotNullStr:
		dslExpr = fmt.Sprintf(`{"bool":{"must":{"exists" : {"field": "%v"}}}}`, colName)
	case sqlparser.IsNullStr:
		dslExpr = fmt.Sprintf(`{"bool":{"must_not":{"exists" : {"field": "%v"}}}}`, colName)
	case sqlparser.IsTrueStr, sqlparser.IsNotFalseStr:
		dslExpr = fmt.Sprintf(`{"term": {"%v": {"value": true}}}`, colName)
	case sqlparser.IsNotTrueStr, sqlparser.IsFalseStr:
		dslExpr = fmt.Sprintf(`{"term": {"%v": {"value": false}}}`, colName)
	default:
	}
	// the root node need to have bool and must
	//if topLevel {
	//	dslExpr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, dslExpr)
	//}
	return dslExpr, nil
}

func buildNestedFuncStrValue(nestedFunc *sqlparser.FuncExpr) (string, error) {
	var result string
	switch nestedFunc.Name.String() {
	case "group_concat":
		for _, nestedExpr := range nestedFunc.Exprs {
			switch nestedVal := nestedExpr.(type) {
			case *sqlparser.StarExpr:
				result += strings.Trim(sqlparser.String(nestedVal), `'`)
			default:
				return "", errors.New("elasticsql: unsupported expression" + sqlparser.String(nestedExpr))
			}
		}
		//TODO support more functions
	default:
		return "", errors.New("elasticsql: unsupported function" + nestedFunc.Name.String())
	}
	return result, nil
}
