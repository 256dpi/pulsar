package main

func main() {
	// run goroutines
	go producer()
	go consumer()
	go printer()
	go debugger()

	select {}
}
