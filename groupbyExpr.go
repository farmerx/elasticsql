package elasticsql

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// handleSelectGroupBy 处置Select 里面的group by
func handleSelectGroupBy(Select *sqlparser.Select, size string) ([]string, []byte, error) {
	var aggMap = make(map[string]interface{}, 0)
	var parentNode *map[string]interface{}
	//解析各路groupby 当然group by 可能不存在
	for index, expr := range Select.GroupBy {
		switch exprV := expr.(type) {
		case *sqlparser.ColName:
			handleGroupByColName(exprV, index, size, &aggMap, &parentNode)
		case *sqlparser.FuncExpr:
			if err := handleGroupByFuncExpr(exprV, index, size, &aggMap, &parentNode); err != nil {
				return nil, nil, err
			}
		}
	}
	//  解析各路group by end。。。。。。。。。。。。。。。。
	funcArr, colArr, err := handleSelectFuncExpr(Select.SelectExprs)
	if err != nil {
		return nil, nil, err
	}
	if err := handleGroupBySelectFuncExpr(funcArr, &aggMap, &parentNode, size); err != nil {
		return nil, nil, err
	}
	if parentNode == nil {
		return colArr, nil, nil
	}
	aggdsl, err := json.Marshal(aggMap)
	return colArr, aggdsl, err
}

func handleGroupBySelectFuncExpr(funcExprArr []*sqlparser.FuncExpr, aggMap *map[string]interface{}, parentNode **map[string]interface{}, size string) error {
	for _, funcExpr := range funcExprArr {
		if *parentNode != nil {
			// 非顶级
			if err := handleGroupByFuncExpr(funcExpr, 0, size, aggMap, parentNode); err != nil {
				return err
			}
		} else {
			// 顶级
			if err := handleGroupByFuncExpr(funcExpr, 0, size, aggMap, parentNode); err != nil {
				return err
			}
		}
	}
	return nil
}

func handleGroupByColName(colName *sqlparser.ColName, index int, size string, aggMap *map[string]interface{}, parentNode **map[string]interface{}) {
	innerMap := make(map[string]interface{})
	// index == 0的情况
	if index == 0 {
		innerMap["terms"] = map[string]interface{}{
			"field": colName.Name.String(),
			"size":  size,
		}
		tmp := *aggMap
		tmp[colName.Name.String()] = &innerMap
		*parentNode = &innerMap
		return
	}
	// index > 0的情况
	innerMap["terms"] = map[string]interface{}{
		"field": colName.Name.String(),
		"size":  0,
	}
	tmp := *parentNode
	(*tmp)["aggregations"] = map[string]interface{}{
		colName.Name.String(): innerMap,
	}
	tmp = &innerMap
	return
}

// handleGroupBYFuncExpr 处置group by 包含方法的
func handleGroupByFuncExpr(funcExpr *sqlparser.FuncExpr, index int, size string, aggMap *map[string]interface{}, parentNode **map[string]interface{}) error {
	innerMap := make(map[string]interface{}, 0)
	switch strings.ToLower(funcExpr.Name.String()) {
	case `stats`:
		if err := beyondSQLStats(funcExpr, &innerMap); err != nil {
			return err
		}
	case `date_histogram`, `histogram`:
		if err := beyondSQLDateHistogram(funcExpr, &innerMap); err != nil {
			return err
		}
	case `range`, `date_range`:
		if err := beyondSQLRange(funcExpr, &innerMap); err != nil {
			return err
		}
	case `count`:
		groupbyFuncExprCount(funcExpr, &innerMap)
	default:
		groupbyDefaultFuncExpr(funcExpr, &innerMap)
	}
	if index == 0 {
		tmp := *aggMap
		tmp[strings.ToLower(funcExpr.Name.String())] = &innerMap
		*parentNode = &innerMap
		return nil
	}
	tmp := *parentNode
	(*tmp)["aggregations"] = map[string]interface{}{strings.ToLower(funcExpr.Name.String()): innerMap}
	tmp = &innerMap
	return nil
}

func groupbyDefaultFuncExpr(funcExpr *sqlparser.FuncExpr, innerMap *map[string]interface{}) {
	tmp := *innerMap
	tmp[strings.ToLower(funcExpr.Name.String())] = map[string]interface{}{
		"field": sqlparser.String(funcExpr.Exprs),
	}
	return
}

func groupbyFuncExprCount(funcExpr *sqlparser.FuncExpr, innerMap *map[string]interface{}) (err error) {
	colName := ``
	for _, expr := range funcExpr.Exprs {
		if funcExpr.Distinct {
			if colName, err = getAliasedExprParenExprColName(expr); err != nil {
				return
			}
			continue
		}
		if sqlparser.String(funcExpr.Exprs) == "*" {
			colName = `_index`
			continue
		}
		if colName, err = getAliasedExprColExprName(expr); err != nil {
			return
		}
	}
	if funcExpr.Distinct {
		tmp := *innerMap
		tmp[`cardinality`] = map[string]interface{}{
			"field": colName,
		}
		return
	}
	tmp := *innerMap
	tmp[`count`] = map[string]interface{}{
		"field": colName,
	}
	return
}

// {
//     "aggs":{
//         "grade_ranges":{
//             "range":{
//                 "field":"grade",
//                 "ranges":[
//                     {"to":60},
//                     {"from":60,"to":80},
//                     {"from":80}]
//                 }
//             }
//         }
// }
// Elasticsearch提供了多种聚合方式，能帮助用户快速的进行信息统计与分类，如何使用Range区间聚合。
func beyondSQLRange(funcExpr *sqlparser.FuncExpr, innerMap *map[string]interface{}) error {
	var field string
	var rangeVal string
	var format string
	for _, expr := range funcExpr.Exprs {
		colName, colVal, _, err := getSelectExprAliasedExprcomparisonExpr(expr)
		if err != nil {
			return err
		}
		if colName == `field` {
			field = colVal
		}
		if colName == "range" {
			rangeVal = colVal
		}
		if colName == "format" {
			format = colVal
		}
	}
	rangeArr := strings.Split(rangeVal, ",")
	var ranges = make([]map[string]interface{}, 0)
	for i := 0; i <= len(rangeArr)-1; i++ {
		if i+1 < len(rangeArr) {
			if format != `` {
				ranges = append(ranges, map[string]interface{}{"from": rangeArr[i], "to": rangeArr[i+1], "format": format})
			} else {
				ranges = append(ranges, map[string]interface{}{"from": rangeArr[i], "to": rangeArr[i+1]})
			}
		} else {
			if format != `` {
				ranges = append(ranges, map[string]interface{}{"from": rangeArr[i], "format": format})
			} else {
				ranges = append(ranges, map[string]interface{}{"from": rangeArr[i]})
			}
		}
	}
	tmp := *innerMap
	tmp["range"] = map[string]interface{}{
		"field":  field,
		"ranges": ranges,
	}
	return nil
}

func beyondSQLDateHistogram(funcExpr *sqlparser.FuncExpr, innerMap *map[string]interface{}) error {
	var (
		field    string
		interval string
		format   string
	)
	// 遍历funcExpr(filed="xxx",interval="1h",format="")
	// the expression in date_histogram must be like a = b format
	for _, expr := range funcExpr.Exprs {
		colName, colVal, _, err := getSelectExprAliasedExprcomparisonExpr(expr)
		if err != nil {
			return err
		}
		if colName == `field` {
			field = colVal
		}
		if colName == "_interval" {
			interval = colVal
		}
		if colName == "format" {
			format = colVal
		}
	}
	tmp := *innerMap
	if format != `` {
		tmp["date_histogram"] = map[string]interface{}{"field": field, "interval": interval, "format": format}
	} else {
		tmp["histogram"] = map[string]interface{}{"field": field, "interval": interval}
	}
	return nil
}

func beyondSQLStats(funcExpr *sqlparser.FuncExpr, innerMap *map[string]interface{}) error {
	var field string
	for _, expr := range funcExpr.Exprs {
		colName, colVal, _, err := getSelectExprAliasedExprcomparisonExpr(expr)
		if err != nil {
			return err
		}
		if colName == `field` {
			field = colVal
		}
	}
	tmp := *innerMap
	tmp[`stats`] = map[string]interface{}{"field": field}
	return nil
}

func getSelectExprAliasedExprcomparisonExpr(expr sqlparser.SelectExpr) (colName string, colVal string, operator string, err error) {
	exprV, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return "", ``, ``, errors.New("ElasticSQL: unsupported expression in group by function")
	}
	comparisonExpr, ok := exprV.Expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return "", ``, ``, errors.New("ElasticSQL: unsupported expression in group by function")
	}
	colname, ok := comparisonExpr.Left.(*sqlparser.ColName)
	if !ok {
		return "", ``, ``, errors.New("ElasticSQL: param error in group by function")
	}
	val, ok := comparisonExpr.Right.(*sqlparser.SQLVal)
	if !ok {
		return "", ``, ``, errors.New("ElasticSQL: param error in group by function")
	}

	replace := strings.NewReplacer("`", "", "'", "", `"`, ``)
	return strings.ToLower(replace.Replace(colname.Name.String())), replace.Replace(string(val.Val)), comparisonExpr.Operator, nil
}

func getAliasedExprColExprName(selectExpr sqlparser.SelectExpr) (colname string, err error) {
	aliasedExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ``, errors.New("ElasticSQL: unsupported expression in select aggs function")
	}

	colName, ok := aliasedExpr.Expr.(*sqlparser.ColName)
	if !ok {
		return ``, errors.New("ElasticSQL: unsupported expression in select aggs function")
	}

	return colName.Name.String(), nil
}

func getAliasedExprParenExprColName(selectExpr sqlparser.SelectExpr) (colname string, err error) {
	aliaseExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
	if !ok {
		return ``, errors.New("ElasticSQL: unsupported expression in select aggs function")
	}

	pexpr, ok := aliaseExpr.Expr.(*sqlparser.ParenExpr)
	if !ok {
		return ``, errors.New("ElasticSQL: unsupported expression in select aggs function")
	}
	colExpr, ok := pexpr.Expr.(*sqlparser.ColName)
	if !ok {
		return ``, errors.New("ElasticSQL: unsupported expression in select aggs function")
	}
	return colExpr.Name.String(), nil
}
