package main

import (
	"bufio"
	"os"

	"./server"
)

func main() {
	// testing metadata server
	os.Setenv("CHUNK_SERVER_PORT", "3000")
	masterNode := server.NewMasterNode("metadata", map[string]interface{}{"port": 8000})
	go masterNode.Run()
	scanner := bufio.NewScanner(os.Stdout)
	scanner.Scan()
}
