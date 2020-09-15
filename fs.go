package main

import (
	"fmt"
	"goSimDFS/client"
	"goSimDFS/server"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
)

var HELP_MESSAGE = fmt.Sprintf(`usage: %s [help] <command> [<args>]
general commands:
help - display this help message 

start - start meta-data server, which also starts chunk server on a background process

kill - stop running servers 

file system commands:
read <filename> - display content of specified filename

write <filename> - create file entry from specified filename on local disk

ls - list available files

stat <filename> - fetch info of file with specified filename

filesize <filename> -  fetch size of file with specified filename 

rename <filename> <new filename> - rename specified file entry 

diskcapacity - fetch sum of leftover disk space on each chunk node

nodestat - fetch total disk size and leftover disk size for each chunk node

stopnode - randomly select and stop a node (simulate a node failure)
`, os.Args[0])

func main() {

	if len(os.Args) > 1 {
		metaPort := os.Getenv("META_SERVER_PORT")
		if len(metaPort) == 0 {
			log.Fatal("META_SERVER_PORT environment variable is not set")
		}

		chunkPort := os.Getenv("CHUNK_SERVER_PORT")
		if len(chunkPort) == 0 {
			log.Fatal("CHUNK_SERVER_PORT environment variable is not set")
		}
		if os.Args[1] == "startserver" {
			port, _ := strconv.Atoi(metaPort)
			masterNode := server.NewMasterServer("metadata", map[string]interface{}{"port": port})
			masterNode.Run()

		} else if os.Args[1] == "start" {
			cmd := exec.Command(os.Args[0], "startserver", os.Getenv("META_SERVER_PORT"), "&")
			cmd.Stdout = os.Stdout
			if err := cmd.Start(); err != nil {
				log.Fatal(err.Error())
			}
			os.Exit(1)
		} else if os.Args[1] == "help" {
			fmt.Println(HELP_MESSAGE)
		} else {
			createConnection(os.Args)
		}

	} else {
		fmt.Println(HELP_MESSAGE)
	}

}

func createConnection(args []string) {
	metaConn, err := net.Dial("tcp", ":"+os.Getenv("META_SERVER_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer metaConn.Close()
	chunkConn, err := net.Dial("tcp", ":"+os.Getenv("CHUNK_SERVER_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}
	defer chunkConn.Close()
	client := client.NewClient(metaConn, chunkConn)

	switch args[1] {
	case "read":
		if len(args) < 3 {
			fmt.Printf("missing argument read <filename>. See '%s help' for commands\n", os.Args[0])
			os.Exit(1)
		}
		filename := args[2]
		fmt.Print(client.Read(filename))
		break
	case "write":
		if len(args) < 3 {
			fmt.Printf("missing argument write <filename>. See '%s help' for commands\n", os.Args[0])
			os.Exit(1)
		}
		filename := args[2]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err.Error())
		}
		client.Write(filename, file)
		break
	case "nodestat":
		if len(args) > 2 {
			id, err := strconv.Atoi(args[2])
			if err != nil {
				log.Fatal(err)
			}
			client.GetNodeStatById(id)
		} else {
			client.GetNodeStat()
		}
		break
	case "kill":
		client.Kill()
		break
	case "stopnode":
		client.StopNode()
		break
	case "filesize":
		if len(args) < 3 {
			fmt.Printf("missing argument filesize <filename>. See '%s help' for commands\n", os.Args[0])
			os.Exit(1)
		}
		client.GetFileSize(args[2])
		break
	case "ls":
		client.ListFiles()
		break
	case "stat":
		if len(args) < 3 {
			fmt.Printf("missing argument stat <filename>. See '%s help' for commands\n", os.Args[0])
			os.Exit(1)
		}
		client.GetFileStat(args[2])
		break
	case "diskcapacity":
		client.GetDiskCapacity()
		break
	case "rename":
		if len(args) < 4 {
			fmt.Printf("missing argument rename <filename> <new filename>. See '%s help' for commands\n", os.Args[0])
			os.Exit(1)
		}
		client.Rename(args[2], args[3])
		break
	default:
		fmt.Printf("%s is not a command. See '%s help'\n", args[1], os.Args[0])
		os.Exit(1)
	}
}
