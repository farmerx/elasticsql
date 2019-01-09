package elasticsql

import (
	"encoding/json"
	"fmt"
	"strings"
)

func buildDSL(queryMapStr, queryFrom, querySize string, aggStr string, orderByArr []string, colArr []string) string {
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

	if len(colArr) > 0 {
		cols, _ := json.Marshal(colArr)

		resultMap["_source"] = string(cols)
	}

	// keep the travesal in order, avoid unpredicted json
	var keySlice = []string{"query", "_source", "from", "size", "sort", "aggregations"}
	var resultArr []string
	for _, mapKey := range keySlice {
		if val, ok := resultMap[mapKey]; ok {
			resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, mapKey, val))
		}
	}
	return "{" + strings.Join(resultArr, ",") + "}"
}
