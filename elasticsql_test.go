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

func Test_ParseSelectSomeField(t *testing.T) {
	table, x, err := esql.SQLConvert(`select name, age, sex from student`)
	if err != nil {
		t.Error(err)
	}
	if x != `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"_source" : ["name","age","sex"],"from" : 0,"size" : 10}` {
		t.Error("不符合预期")
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
}

func Test_ParseSelect(t *testing.T) {
	table, x, err := esql.SQLConvert(`select * from student`)
	if err != nil {
		t.Error(err)
	}
	if x != `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 10}` {
		t.Error("不符合预期")
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
}

func Test_ParseSelectFuncExpr(t *testing.T) {
	table, x, err := esql.SQLConvert(`select avg(age),min(age),max(age),count(student) from student`)
	if err != nil {
		t.Error(err)
	}
	if x != `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"avg":{"avg":{"field":"age"}},"count":{"count":{"field":"student"}},"max":{"max":{"field":"age"}},"min":{"min":{"field":"age"}}}}` {
		t.Error("不符合预期")
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
}

func Test_Count(t *testing.T) {
	table, x, err := esql.SQLConvert(`select count(id) from student`)
	if err != nil {
		t.Error(err)
	}
	if x != `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"count":{"count":{"field":"id"}}}}` {
		t.Error("不符合预期")
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
}

func Test_DistinctCount(t *testing.T) {
	table, x, err := esql.SQLConvert(`select count(distinct(age)) from student`)
	if err != nil {
		t.Error(err)
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
	if x != `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"count":{"cardinality":{"field":"age"}}}}` {
		t.Error("不符合预期")
	}
}

func Test_WhereOperatorCompersion(t *testing.T) {
	table, x, err := esql.SQLConvert(`select count(distinct(age)) from student where class="一班"`)
	if err != nil {
		t.Error(err)
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
	if x != `{"query" : {"bool" : {"must" : [{"term" : {"class" : "一班"}}]}},"from" : 0,"size" : 0,"aggregations" : {"count":{"cardinality":{"field":"age"}}}}` {
		t.Error("不符合预期")
	}
}

func Test_WhereOperatorAnd(t *testing.T) {
	table, x, err := esql.SQLConvert(`select count(distinct(age)) from student where class="一班" and age > 20`)
	if err != nil {
		t.Error(err)
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
	if x != `{"query" : {"bool" : {"must" : [{"term" : {"class" : "一班"}},{"range" : {"age" : {"gt" : "20"}}}]}},"from" : 0,"size" : 0,"aggregations" : {"count":{"cardinality":{"field":"age"}}}}` {
		t.Error("不符合预期")
	}
}

func Test_WhereOperatorAndT(t *testing.T) {
	table, x, err := esql.SQLConvert(`select count(distinct(age)) from student where class="一班" and age > 20`)
	if err != nil {
		t.Error(err)
	}
	if table != `student` {
		t.Error(`不符合预期`)
	}
	if x != `{"query" : {"bool" : {"must" : [{"term" : {"class" : "一班"}},{"range" : {"age" : {"gt" : "20"}}}]}},"from" : 0,"size" : 0,"aggregations" : {"count":{"cardinality":{"field":"age"}}}}` {
		t.Error("不符合预期")
	}
}

// // select count(distinct mid) from test
// SELECT * FROM bank GROUP BY range(pid, 20,25,30,35,40) limit 20
func Test_SQLConverDateRange(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT * FROM student GROUP BY date_range(field="insert_time",format="yyyy-MM-dd" , range ="2014-08-18, 2014-08-17, now-8d, now-7d, now-6d, now")`)
	fmt.Println(table, dsl, err)
}

func Test_SQLConverRange(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT * FROM student GROUP BY date_range(field="grade", range ="0,60,70,80,90,100")`)
	fmt.Println(table, dsl, err)
}

func Test_SQLConverLike(t *testing.T) {
	table, dsl, err := esql.SQLConvert("SELECT FROM `student-a` Where  a like 'x'")
	fmt.Println(table, dsl, err)
}

func Test_SQLConverQueryString(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT * FROM student Where query_string = 'sip:"xxx" AND dip:"xxx"'`)
	fmt.Println(table, dsl, err)
}

func Test_SQLConverStats(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT online FROM online group by stats(field="pid")`)
	fmt.Println(table, dsl, err)
}

func Test_SQLDateHistorgram(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`select * from student group by date_histogram(field="changeTime", _interval="1h", format="yyyy-MM-dd HH:mm:ss")`)
	fmt.Println(table, dsl, err)
}

func Test_SQLHistorgram(t *testing.T) {
	table, dsl, err := esql.SQLConvert(`SELECT online FROM online group by histogram(field="grade", _interval="10")`)
	fmt.Println(table, dsl, err)
}

// // top_hits(field="class", hitssort="age:desc", taglimit = "10", hitslimit = "1", _source="name,age,class")
// func Test_SQLConvertsTopHits(t *testing.T) {
// 	table, dsl, err := esql.SQLConvert(`SELECT top_hits(field="mid") FROM online `)
// 	fmt.Println(table, dsl, err)
// }
