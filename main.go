package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"

	"./server"
)

func main() {
	// testing metadata server

	if len(os.Args) > 1 {
		portString := os.Getenv("META_SERVER_PORT")
		if len(portString) == 0 {
			log.Fatal("META_SERVER_PORT environment variable is not set")
		}
		if os.Args[1] == "startserver" {
			port, _ := strconv.Atoi(portString)
			masterNode := server.NewMasterNode("metadata", map[string]interface{}{"port": port})
			masterNode.Run()

		} else if os.Args[1] == "start" {
			cmd := exec.Command("./main", "startserver", os.Getenv("META_SERVER_PORT"), "&")
			cmd.Stdout = os.Stdout
			if err := cmd.Start(); err != nil {
				log.Fatal(err.Error())
			}
			os.Exit(1)
		} else {
			createConnection(os.Args)
		}

	} else {
		fmt.Println("usuage: ./main start")
	}

}

func createConnection(args []string) {
	conn, err := net.Dial("tcp", ":"+os.Getenv("META_SERVER_PORT"))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)
	err = enc.Encode(server.Message{Command: args[1], Args: args[2:]})
	if err != nil {
		log.Fatalln(err.Error())
	}
	var returnMsg string
	err = dec.Decode(&returnMsg)
	if err != nil {
		if err == io.EOF {
		}
		log.Fatal("decode error: ", err.Error())
	}
	log.Println(returnMsg)
}
