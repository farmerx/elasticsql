package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// ElasticSQL ..
type ElasticSQL struct {
}

// NewElasticSQL ...
func NewElasticSQL() *ElasticSQL {
	return new(ElasticSQL)
}

// SQLConvert sql convert to elasticsearch dsl
func (esql *ElasticSQL) SQLConvert(sql string) (table string, dsl string, err error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return ``, ``, errors.New(`ElastciSQL: ` + err.Error())
	}
	switch v := stmt.(type) {
	case *sqlparser.Select:
		return handleParseSelect(v)
	default:
		return ``, ``, errors.New(`ElasticSQL: Support SQL where parsing only`)
	}
}

// handleParseSelect  parse sql select
func handleParseSelect(selectStmt *sqlparser.Select) (table string, dsl string, err error) {
	// 获取from
	if table, err = getFromTable(selectStmt); err != nil {
		return ``, ``, err
	}
	from, size := getFromAndSize(selectStmt)

	// 解析where
	querydsl := ``
	if selectStmt.Where != nil {
		if querydsl, err = handleSelectWhere(&selectStmt.Where.Expr, true); err != nil {
			return ``, ``, err
		}
	}
	if querydsl == `` {
		querydsl = `{"bool" : {"must": [{"match_all" : {}}]}}`
	}

	aggsdsl, err := handleSelectGroupBy(selectStmt, size)
	if err != nil {
		return ``, ``, err
	}
	var orderByArr []string
	if aggsdsl != nil {
		from, size = "0", "0"
	} else {
		// Handle order by
		// when executating aggregations, order by is useless
		for _, orderByExpr := range selectStmt.OrderBy {
			orderByStr := fmt.Sprintf(`{"%v": "%v"}`, sqlparser.String(orderByExpr.Expr), orderByExpr.Direction)
			orderByArr = append(orderByArr, orderByStr)
		}
	}
	return table, buildDSL(querydsl, from, size, string(aggsdsl), []string{}), nil
}

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

// extract func expressions from select exprs
func handleSelectFuncExpr(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName, error) {
	var colArr []*sqlparser.ColName
	var funcArr []*sqlparser.FuncExpr
	for _, selectVal := range sqlSelect {
		expr, ok := selectVal.(*sqlparser.AliasedExpr)
		if !ok {
			continue // no need to handle, star expression * just skip is ok
		}
		switch exprV := expr.Expr.(type) {
		case *sqlparser.ColName:
			colArr = append(colArr, exprV)
		case *sqlparser.FuncExpr:
			funcArr = append(funcArr, exprV)
		default:
			// ignore
			continue
		}
	}
	return funcArr, colArr, nil
}

// getSqlFrom
// 如果From 含有特殊 "-","*"等用"``" 引起来
func getFromTable(selectStmt *sqlparser.Select) (string, error) {
	// 处理
	if len(selectStmt.From) != 1 {
		return ``, errors.New("ElasticSQL: multiple SQL from currently not supported")
	}
	return sqlparser.String(selectStmt.From), nil
}

// getFromAndSize ...
// get limit and offset
func getFromAndSize(selectStmt *sqlparser.Select) (from, size string) {
	from, size = "0", "10"
	if selectStmt.Limit == nil {
		return
	}
	if selectStmt.Limit.Offset != nil {
		from = sqlparser.String(selectStmt.Limit.Offset)
	}
	size = sqlparser.String(selectStmt.Limit.Rowcount)
	return
}

// handleSelectWhere ....
// 解析sql where
func handleSelectWhere(expr *sqlparser.Expr, topLevel bool) (string, error) {

	if expr == nil {
		return "", errors.New("ElasticSQL: SQL where exprssion can not be empty")
	}

	switch exprVal := (*expr).(type) {
	case *sqlparser.AndExpr: // where and
		resultStr, err := handleAndExpr(exprVal)
		if err != nil {
			return ``, err
		}
		return fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr), nil
	case *sqlparser.OrExpr: // wehew or
		resultStr, err := handleOrExpr(exprVal)
		if err != nil {
			return ``, err
		}
		return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, resultStr), nil
	case *sqlparser.ComparisonExpr:
		return handleComparisonExpr(exprVal, topLevel)
	case *sqlparser.RangeCond:
		return handleRangeCond(exprVal, topLevel)
	case *sqlparser.ParenExpr:
		return handleSelectWhere(&exprVal.Expr, topLevel)
	case *sqlparser.NotExpr:
		return ``, errors.New("ElasticSQL: not expression currently not supported")
	default:
		return ``, errors.New("ElasticSQL: Such expression currently not supported")
	}
}
