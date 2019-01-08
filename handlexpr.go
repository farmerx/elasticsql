package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// handleAndExpr 。。。
func handleAndExpr(andExpr *sqlparser.AndExpr) (string, error) {
	leftStr, err := handleSelectWhere(&andExpr.Left, false)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&andExpr.Right, false)
	if err != nil {
		return "", err
	}
	if leftStr == "" || rightStr == "" {
		return leftStr + rightStr, nil
	}
	return leftStr + `,` + rightStr, nil
}

// handleAndExpr ...
func handleOrExpr(orExpr *sqlparser.OrExpr) (string, error) {
	leftStr, err := handleSelectWhere(&orExpr.Left, false)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&orExpr.Right, false)
	if err != nil {
		return "", err
	}
	if leftStr == "" || rightStr == "" {
		return leftStr + rightStr, nil
	}
	return leftStr + `,` + rightStr, nil
}

func handleRangeCond(rangeCond *sqlparser.RangeCond, topLevel bool) (string, error) {
	// time_event between a and b
	colName, ok := rangeCond.Left.(*sqlparser.ColName)
	if !ok {
		return "", errors.New("ElasticSQL: rangeCond expression column name missing")
	}
	replace := strings.NewReplacer("`", "", `'`, ``)
	resultStr := fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v", "to" : "%v"}}}`, replace.Replace(sqlparser.String(colName)), replace.Replace(sqlparser.String(rangeCond.From)), replace.Replace(sqlparser.String(rangeCond.To)))
	if topLevel {
		resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}
	return resultStr, nil
}
