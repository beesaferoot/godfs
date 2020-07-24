package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"./server"
)

func main() {
	// testing metadata server
	masterNode := server.NewMasterNode("metadata", map[string]interface{}{"port": 8000})
	os.Setenv("META_SERVER_PORT", strconv.Itoa(masterNode.PORT))

	go masterNode.Run()
	time.Sleep(time.Second * 2)
	createConnection()
}

func createConnection() {
	conn, err := net.Dial("tcp", ":"+os.Getenv("META_SERVER_PORT"))
	if err != nil {
		log.Fatalln(err.Error())
	}
	log.Printf("connected to network")
	defer conn.Close()

	enc := gob.NewEncoder(conn)
	buf := bufio.NewScanner(os.Stdin)

	for {
		if ok := buf.Scan(); !ok {
			log.Fatalln(err.Error())
		}

		message := strings.Split(buf.Text(), " ")
		fmt.Println(message)
		err := enc.Encode(server.Message{Command: message[0], Args: message[1:]})
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

}
