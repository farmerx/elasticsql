package elasticsql

import (
	"fmt"
	"sync"
	"testing"
)

var esql *ElasticSQL
var once sync.Once

func init() {

	esql = NewElasticSQL()

}

// func Test_SQLConver(t *testing.T) {
// 	_, x, err := esql.SQLConvert(`select avg(mid) from test where a=1 and b="c" and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800' and process_id > 1 group by class, date_range(field="insert_time",format="yyyy-MM-dd" , range ="2014-08-18,2014-08-17,now-8d,now-7d,now-6d,now") limit 20`)
// 	fmt.Println(x, err)
// }

func Test_ParseSelect(t *testing.T) {
	_, x, err := esql.SQLConvert(`select name, age, sex from student`)
	fmt.Println(x, err)
}

// // select count(distinct mid) from test
// // SELECT * FROM bank GROUP BY range(pid, 20,25,30,35,40) limit 20
// func Test_SQLConverGroup(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT online FROM online GROUP BY date_range(field="insert_time",format="yyyy-MM-dd" ,"2014-08-18","2014-08-17","now-8d","now-7d","now-6d","now")`)
// 	fmt.Println(table, dsl, err)
// }

// func Test_SQLConverLike(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT online FROM online Where  a like 'x'`)
// 	fmt.Println(table, dsl, err)
// }

// func Test_SQLConverQueryString(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT online FROM online Where query_string = 'sip:"xxx" AND dip:"xxx"'`)
// 	fmt.Println(table, dsl, err)
// }

// func Test_SQLConverStats(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT online FROM online group by stats(field="pid")`)
// 	fmt.Println(table, dsl, err)
// }

// // top_hits(field="class", hitssort="age:desc", taglimit = "10", hitslimit = "1", _source="name,age,class")
// func Test_SQLConvertsTopHits(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT top_hits(field="mid") FROM online `)
// 	fmt.Println(table, dsl, err)
// }
