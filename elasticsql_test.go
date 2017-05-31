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

// select count(distinct mid) from test
// SELECT * FROM bank GROUP BY range(pid, 20,25,30,35,40) limit 20
func Test_SQLConverGroup(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT online FROM online GROUP BY date_range(field="insert_time",format="yyyy-MM-dd" ,"2014-08-18","2014-08-17","now-8d","now-7d","now-6d","now")`)
	fmt.Println(table, dsl, err)
}
