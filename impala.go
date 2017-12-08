package main

import (
	"fmt"
	//"log"
	"time"

	"github.com/koblas/impalathing"
)

func main() {
	host := "impala-host"
	port := 21000

	con, err := impalathing.Connect(host, port, impalathing.DefaultOptions)

	if err != nil {
		fmt.Println("Error connecting", err)
		return
	}

	query, err := con.Query("SELECT user_id, action, yyyymm FROM engagements LIMIT 10000")

	startTime := time.Now()
	total := 0
	for query.Next() {
		var (
			user_id string
			action  string
			yyyymm  int
		)

		query.Scan(&user_id, &action, &yyyymm)
		total += 1

		fmt.Println(user_id, action)
	}

	fmt.Printf("Fetch %d rows(s) in %.2fs", total, time.Duration(time.Since(startTime)).Seconds())
}
