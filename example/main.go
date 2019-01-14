package main

func main() {
	// run goroutines
	go producer()
	go subscriber()
	go printer()
	go debugger()

	select {}
}
