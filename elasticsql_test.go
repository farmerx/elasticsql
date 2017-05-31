package elasticsql

import (
	"fmt"
	"testing"
)

var esql *ElasticSQL

func init() {
	esql = NewElasticSQL(InitOptions{})
}

func Test_SQLConver(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`select * from test where a="b" and c ="2" or d="1" and age between 2 and 3 group by mid,gid order by mid desc `)
	fmt.Println(table, dsl, err)
}

func Test_SQLConverGroup(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`select avg(age),min(age),max(age) from test group by class limit 10`)
	fmt.Println(table, dsl, err)
}
