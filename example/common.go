package main

import (
	"fmt"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/montanaflynn/stats"
)

var send int64
var recv int64

var diff []float64

func printer() {
	// create ticker
	ticker := time.Tick(time.Second)

	for {
		// await signal
		<-ticker

		// get data
		r := recv
		s := send
		d := diff

		// get stats
		min, _ := stats.Min(d)
		max, _ := stats.Max(d)
		mean, _ := stats.Mean(d)
		p90, _ := stats.Percentile(d, 90)
		p95, _ := stats.Percentile(d, 95)
		p99, _ := stats.Percentile(d, 99)

		// print rate
		fmt.Printf("send: %d msg/s, ", s)
		fmt.Printf("recv %d msgs/s, ", r)
		fmt.Printf("min: %.2fms, ", min)
		fmt.Printf("mean: %.2fms, ", mean)
		fmt.Printf("p90: %.2fms, ", p90)
		fmt.Printf("p95: %.2fms, ", p95)
		fmt.Printf("p99: %.2fms, ", p99)
		fmt.Printf("max: %.2fms\n", max)

		// reset data
		recv = 0
		send = 0
		diff = nil
	}
}

func debugger() {
	_ = http.ListenAndServe("localhost:6060", nil)
}
