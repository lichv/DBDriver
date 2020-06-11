Package collectlinks is useful for only one task:

Given a response from `http.Get` it will use parse the page and
return to you a slice of all the href links found.

Usage:

    package main

    import (
      "fmt"
      _ "github.com/go-sql-driver/mysql"
      "github.com/lichv/go-dbDriver/DBDriver"
    )

    func main() {
      postgreDriver := DBDriver.InitPostgreDriver("localhost",5432,"adminb","123456","data")
      insert, err2 := postgreDriver.Insert("article", map[string]interface{}{"title": "测试一个标题", "content": "测试一个内容"})
      if err2 != nil {
        fmt.Println(err2)
      }
      fmt.Println(insert)
    }


Running that will output:

   [http://twitter.com/thebarrytone http://txti.es]
