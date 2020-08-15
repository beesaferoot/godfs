package server

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Copy struct {
	node  int
	addr  int
	valid bool
}

type Chunk interface {
	GetMaxSize() int
	Read() []byte
	Write([]byte)
	Size() int
}

type ChunkFile struct {
	data    []byte
	valid   bool
	maxSize int
}

type ChunkEntry interface {
	Read() []Copy
	stopNode(int)
}

type ChunkMetadata struct {
	index  int
	copies []Copy
}

type ChunkServer struct {
	serverName  string
	socket      net.Listener
	CHUNKSIZE   int
	NODEPERRACK int
	RACKNUMBER  int
	data        string
	nodes       []DataNode
	PORT        int
	encoder     *gob.Encoder
	decoder     *gob.Decoder
}

type DataNode interface {
	GetSize() int
	Run()
	Write(int, <-chan []byte) int
	Kill()
	Read(int) string
	Delete(int) (bool, error)
	IsRunning() bool
}

type Node struct {
	id       int
	size     int
	count    int
	isKilled bool
	content  []Chunk
	mutex    sync.Mutex
}

func (c *Copy) stopNode() {
	c.valid = false
}

func (c *ChunkFile) Read() []byte {
	return c.data
}

func (c *ChunkFile) Write(fragment []byte) {
	c.data = fragment
}

func (c *ChunkFile) Size() int {
	return len(c.data)
}

// func (c ChunkFile) Delete() {
// 	c.data = make([]byte, c.GetMaxSize())
// }

func (c *ChunkFile) GetMaxSize() int {
	return c.maxSize
}

func (c *ChunkMetadata) stopNode(nodeID int) {

	for _, chunkCopy := range c.copies {
		if chunkCopy.node == nodeID {
			chunkCopy.stopNode()
			break
		}
	}
}

func (c *ChunkMetadata) Read() []Copy {
	return c.copies
}

func (c *ChunkServer) sendMsg(msg *Message) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%s", os.Getenv("META_SERVER_PORT")))
	defer conn.Close()
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(conn)
	err = enc.Encode(msg)
	if err != nil {
		return fmt.Errorf("Error: could not accept incomming request: %v", err.Error())
	}

	return nil

}

func (c *ChunkServer) Run() {
	var err error
	c.socket, err = net.Listen("tcp", fmt.Sprintf(":%d", c.PORT))
	if err != nil {
		log.Fatalf("unable to start %s server: %v\n", c.serverName, err.Error())
	}
	fmt.Printf("starting %v server at port %d\n", c.serverName, c.PORT)
	fmt.Printf("listening on port %d\n", c.PORT)

	for {
		conn, err := c.socket.Accept()
		if err != nil {
			log.Fatal(err.Error())
		}
		c.encoder = gob.NewEncoder(conn)
		c.decoder = gob.NewDecoder(conn)
		go c.handleConnection(conn)

	}
}

func (c *ChunkServer) GetInfo() string {

	return fmt.Sprintf(`server type:          %s server
					   total avialable nodes: %d
					   nodes per rack:        %d
					   total available racks: %d
					   running nodes:         %d`,
		c.serverName, len(c.nodes), c.RACKNUMBER, c.NODEPERRACK,
		c.RunningNodes())
}

func (c *ChunkServer) RunningNodes() int {
	var totalRunningNodes int
	for _, node := range c.nodes {
		if node.IsRunning() {
			totalRunningNodes++
		}
	}
	return totalRunningNodes
}

func (c *ChunkServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	var msg Message
	var err error
	for {
		err = c.decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("decode error: ", err.Error())
			c.encoder.Encode(err.Error())
			break
		}
		c.handleClientCommands(&msg)
	}
}

func (c *ChunkServer) handleClientCommands(msg *Message) {
	var err error
	switch msg.Command {
	case "read":
		var entries []ChunkEntry
		err = c.decoder.Decode(&entries)
		if err != nil {
			c.encoder.Encode(err.Error())
			break
		}
		go c.handleReadConnection(&entries)
		break
	case "write":
		var entry FileEntry
		err = c.decoder.Decode(&entry)
		if err != nil {
			_ = c.encoder.Encode(err.Error())
			break
		}
		go c.handleWriteConnection(entry)
		break
	case "killnode":
		nodeID, _ := strconv.Atoi(msg.Args[0])
		go c.handleKillConnection(nodeID)
	case "killserver":
		os.Exit(1)
		break
	default:
		break
	}
}

func (c *ChunkServer) handleReadConnection(entries *[]ChunkEntry) {
	var fileString string
	var foundvalidCopy bool
	for _, entry := range *entries {
		foundvalidCopy = false
		copies := entry.Read()
		for _, copy := range copies {
			if copy.valid {
				foundvalidCopy = true
				fileString += c.nodes[copy.node].Read(copy.addr)
				break
			}
		}
		if !foundvalidCopy {
			_ = c.encoder.Encode(fmt.Errorf("missing chunk for the specified"))
			break
		}
	}
	_ = c.encoder.Encode(fileString)
}

func (c *ChunkServer) handleWriteConnection(entry FileEntry) {
	var buf = make([]byte, c.CHUNKSIZE)
	dataChannel := make(chan []byte, 3)
	var chunkCopies []Copy
	// ensure no chunk copies exists for this file entry
	for _, chunk := range entry.Read() {
		copies := chunk.Read()
		if len(copies) > 0 {
			for _, copy := range copies {
				_, _ = c.nodes[copy.node].Delete(copy.addr)
			}
		}
	}
	entry.DeleteChunks()
	var err error
	nodeID := c.pickWriteNode()

	for {
		err = c.decoder.Decode(&buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("decode error: ", err.Error())
			break
		}
		// add 3 copies of buffered chunk
		dataChannel <- buf
		dataChannel <- buf
		dataChannel <- buf
		chunkCopies = append(chunkCopies, c.hanleDataWrite(nodeID, dataChannel))
		of1, of2 := c.computeReplica(nodeID)
		chunkCopies = append(chunkCopies, c.hanleDataWrite(nodeID+of1, dataChannel))
		chunkCopies = append(chunkCopies, c.hanleDataWrite(nodeID+of2, dataChannel))
		entry.Write(nodeID, &chunkCopies)
	}
	go func() {
		err = c.sendMsg(&Message{Command: "updateFileEntry"})
		if err != nil {
			log.Println(err.Error())
		}
		_ = c.encoder.Encode(entry.Read())
	}()

}

func (c *ChunkServer) handleKillConnection(nodeID int) {
	node := c.nodes[nodeID]
	node.Kill()
	_ = c.encoder.Encode(fmt.Sprintf("node with id %d successfully killed", nodeID))
}

func (c *ChunkServer) hanleDataWrite(nodeID int, dataChannel <-chan []byte) Copy {

	node := c.nodes[nodeID]
	addr := node.Write(nodeID, dataChannel)
	return Copy{node: nodeID, addr: addr, valid: true}
}

func (c *ChunkServer) computeReplica(nodeID int) (int, int) {

	offset1 := 1
	offset2 := c.NODEPERRACK
	if (nodeID-offset1) > 0 && (nodeID-offset1) < c.RACKNUMBER {
		offset1 = -offset1
	}
	if (nodeID-offset2) > 0 && (nodeID-offset2) < c.RACKNUMBER {
		offset2 = -offset2
	}
	return offset1, offset2
}

func (c *ChunkServer) pickWriteNode() int {

	rand.Seed(time.Now().UnixNano())
	var availableNodes []int

	for index, node := range c.nodes {
		if node.IsRunning() {
			availableNodes = append(availableNodes, index)
		}
	}

	return availableNodes[rand.Intn(len(availableNodes)-1)]

}

func (n *Node) GetSize() int {
	var size int
	for _, chunk := range n.content {
		size += chunk.Size()
	}
	return size
}

func (n *Node) Run() {
	n.isKilled = false
}

func (n *Node) Read(offset int) string {
	return string(n.content[offset].Read())
}

func (n *Node) Kill() {
	n.isKilled = true
}

func (n *Node) Delete(addr int) (bool, error) {
	n.mutex.Lock()
	n.content[addr] = n.content[len(n.content)-1]
	n.content = n.content[:len(n.content)-1]
	n.mutex.Unlock()
	return true, nil
}

func (n *Node) Write(nodeID int, dataChannel <-chan []byte) int {
	data := <-dataChannel
	n.content[nodeID].Write(data)
	return len(n.content) - 1
}

func (n *Node) IsRunning() bool {
	return !n.isKilled
}
