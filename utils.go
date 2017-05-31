package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func buildDSL(queryMapStr, queryFrom, querySize string, aggStr string, orderByArr []string) string {
	resultMap := make(map[string]interface{})
	resultMap["query"] = queryMapStr
	resultMap["from"] = queryFrom
	resultMap["size"] = querySize
	if len(aggStr) > 0 {
		resultMap["aggregations"] = aggStr
	}

	if len(orderByArr) > 0 {
		resultMap["sort"] = fmt.Sprintf("[%v]", strings.Join(orderByArr, ","))
	}

	// keep the travesal in order, avoid unpredicted json
	var keySlice = []string{"query", "from", "size", "sort", "aggregations"}
	var resultArr []string
	for _, mapKey := range keySlice {
		if val, ok := resultMap[mapKey]; ok {
			resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, mapKey, val))
		}
	}
	return "{" + strings.Join(resultArr, ",") + "}"
}

func handleAndExpr(andExpr *sqlparser.AndExpr, expr *sqlparser.BoolExpr) (string, error) {
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}
	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}
	return resultStr, nil
}

func handleOrExpr(orExpr *sqlparser.OrExpr, expr *sqlparser.BoolExpr) (string, error) {
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right
	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}

	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}
	return resultStr, nil
}
func buildNestedFuncStrValue(nestedFunc *sqlparser.FuncExpr) (string, error) {
	var result string
	switch string(nestedFunc.Name) {
	case "group_concat":
		for _, nestedExpr := range nestedFunc.Exprs {
			switch nestedExpr.(type) {
			case *sqlparser.NonStarExpr:
				nonStarExpr := nestedExpr.(*sqlparser.NonStarExpr)
				result += strings.Trim(sqlparser.String(nonStarExpr), `'`)
			default:
				return "", errors.New("elasticsql: unsupported expression" + sqlparser.String(nestedExpr))
			}
		}
		//TODO support more functions
	default:
		return "", errors.New("elasticsql: unsupported function" + string(nestedFunc.Name))
	}
	return result, nil
}

func handleComparisonExpr(comparisonExpr *sqlparser.ComparisonExpr, topLevel bool) (string, error) {
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("elasticsql: invalid comparison expression, the left must be a column name")
	}
	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	rightStr, err := getComparisonExprRight(comparisonExpr)
	if err != nil {
		return ``, err
	}
	resultStr := handlecomparisonExprOperator(comparisonExpr, rightStr, colNameStr)
	// the root node need to have bool and must
	if topLevel {
		resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}
	return resultStr, nil
}

func handlecomparisonExprOperator(comparisonExpr *sqlparser.ComparisonExpr, rightStr, colNameStr string) (resultStr string) {
	switch comparisonExpr.Operator {
	case ">=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v"}}}`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"to" : "%v"}}}`, colNameStr, rightStr)
	case "=":
		resultStr = fmt.Sprintf(`{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}`, colNameStr, rightStr)
	case ">":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"gt" : "%v"}}}`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"lt" : "%v"}}}`, colNameStr, rightStr)
	case "!=":
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : [{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}]}}`, colNameStr, rightStr)
	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"terms" : {"%v" : [%v]}}`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}`, colNameStr, rightStr)
	case "not like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}}}`, colNameStr, rightStr)
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"terms" : {"%v" : [%v]}}}}`, colNameStr, rightStr)
	}
	return resultStr
}

func getComparisonExprRight(comparisonExpr *sqlparser.ComparisonExpr) (rightStr string, err error) {
	switch comparisonExpr.Right.(type) {
	case sqlparser.StrVal:
		rightStr = sqlparser.String(comparisonExpr.Right)
		rightStr = strings.Trim(rightStr, `'`)
	case sqlparser.NumVal:
		rightStr = sqlparser.String(comparisonExpr.Right)
	case *sqlparser.FuncExpr:
		// parse nested
		funcExpr := comparisonExpr.Right.(*sqlparser.FuncExpr)
		if rightStr, err = buildNestedFuncStrValue(funcExpr); err != nil {
			return "", err
		}
	case *sqlparser.ColName:
		return "", errors.New("elasticsql: column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(comparisonExpr.Right)
	default:
		// cannot reach here
	}
	return rightStr, nil
}

// handleGroupBYFuncExpr 处置group by 包含方法的
func handleGroupByFuncExpr(funcExpr *sqlparser.FuncExpr) (map[string]interface{}, error) {
	innerMap := make(map[string]interface{}, 0)
	switch string(funcExpr.Name) {
	case `date_histogram`:
		var field string
		interval := "1h"
		format := "yyyy-MM-dd HH:mm:ss"
		// 遍历funcExpr(filed="xxx",interval="1h",format="")
		// the expression in date_histogram must be like a = b format
		for _, expr := range funcExpr.Exprs {
			nonStarExpr, ok := expr.(*sqlparser.NonStarExpr)
			if !ok {
				return nil, errors.New("elasticsql: unsupported expression in date_histogram")
			}
			comparisonExpr, ok := nonStarExpr.Expr.(*sqlparser.ComparisonExpr)
			if !ok {
				return nil, errors.New("elasticsql: unsupported expression in date_histogram")
			}
			left, ok := comparisonExpr.Left.(*sqlparser.ColName)
			if !ok {
				return nil, errors.New("elaticsql: param error in date_histogram")
			}
			rightStr := sqlparser.String(comparisonExpr.Right)
			rightStr = strings.Replace(rightStr, `'`, ``, -1)
			if string(left.Name) == "field" {
				field = rightStr
			}
			if string(left.Name) == "interval" {
				interval = rightStr
			}
			if string(left.Name) == "format" {
				format = rightStr
			}
		}
		innerMap["date_histogram"] = map[string]interface{}{
			"field":    field,
			"interval": interval,
			"format":   format,
		}
		return innerMap, nil
	}
	return nil, errors.New(`elasticsql: agg not supported yet`)
}

func handleGroupByColName(colName *sqlparser.ColName, index int, size string) map[string]interface{} {
	innerMap := make(map[string]interface{})
	if index == 0 {
		innerMap["terms"] = map[string]interface{}{
			"field": string(colName.Name),
			"size":  size,
		}
	} else {
		innerMap["terms"] = map[string]interface{}{
			"field": string(colName.Name),
			"size":  0,
		}
	}
	return innerMap
}

// extract func expressions from select exprs
func handleSelectExpr(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName, error) {
	var colArr []*sqlparser.ColName
	var funcArr []*sqlparser.FuncExpr
	for _, v := range sqlSelect {
		// non star expressioin means column name
		// or some aggregation functions
		expr, ok := v.(*sqlparser.NonStarExpr) // colName 直接放弃
		if !ok {
			continue // no need to handle, star expression * just skip is ok
		}
		// NonStarExpr start
		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			funcArr = append(funcArr, funcExpr)
		case *sqlparser.ColName:
			continue
		default:
			// ignore
		}
		// starExpression like *, table.* should be ignored
		// cause it is meaningless to set fields in elasticsearch aggs
	}
	return funcArr, colArr, nil
}

func handleSelectExprGroupBy(SelectExprs sqlparser.SelectExprs) (map[string]interface{}, error) {
	funcExprArr, _, err := handleSelectExpr(SelectExprs)
	if err != nil {
		return nil, err
	}
	var innerAggMap = make(map[string]interface{}, 0)
	for _, v := range funcExprArr {
		aggName := strings.ToUpper(string(v.Name)) + `(` + sqlparser.String(v.Exprs) + `)`
		switch string(v.Name) {
		case "count":
			//count need to distingush * and normal field name
			if sqlparser.String(v.Exprs) == "*" {
				innerAggMap[aggName] = map[string]interface{}{
					"count": map[string]string{
						"field": "_index",
					},
				}
			} else {
				innerAggMap[aggName] = map[string]interface{}{
					"count": map[string]string{
						"field": sqlparser.String(v.Exprs),
					},
				}
			}
		default:
			innerAggMap[aggName] = map[string]interface{}{
				string(v.Name): map[string]string{
					"field": sqlparser.String(v.Exprs),
				},
			}
		}
	}
	return innerAggMap, nil
}
