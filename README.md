Overview
-----------
[![Build Status](https://travis-ci.org/cch123/elasticsql.svg?branch=master)](https://travis-ci.org/farmerx/essql)
[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/farmerx/essql)
[![Coverage Status](https://coveralls.io/repos/github/cch123/elasticsql/badge.svg?branch=master)](https://coveralls.io/github/farmerx/essql?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/cch123/elasticsql)](https://goreportcard.com/report/github.com/farmerx/essql)

> ElasticSQL package converts SQL to ElasticSearch DSL

SQL Features Support:

- [x] SQL Select
- [x] SQL Where
- [x] SQL Order By
- [x] SQL Group By
- [x] SQL AND & OR
- [x] SQL Like & NOT Like
- [ ] SQL COUNT distinct
- [x] SQL In & Not In
- [x] SQL Between
- [x] SQL avg()、count(*), count(field), min(field), max(field)

Beyond SQL Features Support：
- [ ] ES TopHits
- [x] ES date_histogram
- [ ] ES STATS



*Improvement : now the query DSL is much more flat*

Usage
-------------

> go get github.com/farmerx/essql

Demo :
```go
package main

import (
    "fmt"
    "github.com/farmerx/elasticSQL"
)

var sql = `
select * from test where a=1 and b="c" and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800' and process_id > 1 order by id desc limit 100,10
`

var sql2= `
  select avg(age),min(age),max(age),count(student) from test group by class limit 10
`
var sql3= `
  select * from test group by class,student limit 10
`
var sql4 = `
  select * from test group by date_histogram(field="changeTime",interval="1h",format="yyyy-MM-dd HH:mm:ss")
`


func main() {
    esql := essql.NewElasticSQL(essql.InitOptions{})
    table, dsl, err := esql.SQLConvert(sql)
	fmt.Println(table, dsl, err)
}

```
License
-----------
MIT

