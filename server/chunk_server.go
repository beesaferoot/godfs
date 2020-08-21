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
	Node  int
	Addr  int
	Valid bool
	Size  int
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
	Id() int
	Read() []Copy
	stopNode(int)
	Size() int
}

type ChunkMetadata struct {
	Index  int
	Copies []Copy
}

type ChunkServer struct {
	serverName  string
	socket      net.Listener
	CHUNKSIZE   int
	NODEPERRACK int
	RACKNUMBER  int
	nodes       []DataNode
	PORT        int
	encoder     *gob.Encoder
	decoder     *gob.Decoder
}

type DataNode interface {
	GetSize() int
	Run()
	Write(<-chan []byte) (int, int)
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
	c.Valid = false
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

func (c *ChunkFile) GetMaxSize() int {
	return c.maxSize
}

func (c *ChunkMetadata) stopNode(nodeID int) {

	for _, chunkCopy := range c.Copies {
		if chunkCopy.Node == nodeID {
			chunkCopy.stopNode()
			break
		}
	}
}

func (c *ChunkMetadata) Read() []Copy {
	return c.Copies
}

func (c *ChunkMetadata) Id() int {
	return c.Index
}

func (c *ChunkMetadata) Size() int {
	if len(c.Copies) > 0 {
		return c.Copies[0].Size
	}
	return 0
}

func NewChunkServer(serverName string, serverConfig map[string]interface{}) *ChunkServer {

	var newChunkServer = ChunkServer{serverName: serverName}
	portString, _ := serverConfig["port"].(string)
	newChunkServer.PORT, _ = strconv.Atoi(portString)
	newChunkServer.NODEPERRACK, _ = serverConfig["NO_PER_RACK"].(int)
	newChunkServer.CHUNKSIZE, _ = serverConfig["chunksize"].(int)
	nodesCount, _ := serverConfig["nodes"].(int)
	newChunkServer.RACKNUMBER = nodesCount / newChunkServer.NODEPERRACK

	for i := 0; i < serverConfig["nodes"].(int); i++ {
		newChunkServer.nodes = append(newChunkServer.nodes, &Node{})
	}
	return &newChunkServer
}

func (c *ChunkServer) sendMsg(msg *Message) error {
	conn, err := net.Dial("tcp", ":"+os.Getenv("META_SERVER_PORT"))
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

	return fmt.Sprintf(`server type: %s server
total avialable nodes: %d
nodes per rack:        %d
total available racks: %d
running nodes:         %d`,
		c.serverName, len(c.nodes), c.NODEPERRACK, c.RACKNUMBER,
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
			break
		}
		c.handleClientCommands(&msg)
	}
}

func (c *ChunkServer) handleClientCommands(msg *Message) {
	var err error
	switch msg.Command {
	case "read":
		var entry File
		err = c.decoder.Decode(&entry)
		if err != nil {
			if err != io.EOF {
				log.Println(err.Error())
			}
			break
		}
		c.handleReadConnection(entry.Read())
		break
	case "write":
		var entry File
		err = c.decoder.Decode(&entry)
		if err != nil {
			log.Println(err.Error())
			break
		}
		c.handleWriteConnection(&entry)
		break
	case "killnode":
		nodeID, _ := strconv.Atoi(msg.Args[0])
		c.handleKillConnection(nodeID)
		break
	case "nodestat":
		var rmsg struct {
			Result string
			Err    string
		}
		rmsg.Result = c.GetInfo()
		err = c.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	default:
		break
	}
}

func (c *ChunkServer) handleReadConnection(entries []ChunkEntry) {
	var fileString string
	var foundvalidCopy bool
	var err error
	for _, entry := range entries {
		foundvalidCopy = false
		copies := entry.Read()
		for _, copy := range copies {
			if copy.Valid {
				foundvalidCopy = true
				fileString += c.nodes[copy.Node].Read(copy.Addr)
				break
			}
		}
		if !foundvalidCopy {
			err = c.encoder.Encode("no valid chunk data found for file entry")
			if err != nil {
				log.Println(err.Error())
			}
			break
		}
	}
	if err == nil {
		err := c.encoder.Encode(fileString)
		if err != nil {
			log.Println(err.Error())
		}
	}

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
				ok, err := c.nodes[copy.Node].Delete(copy.Addr)
				if ok {
					log.Println("deleted chunk copy at address ", copy.Addr)
				} else {
					log.Println(err.Error())
				}
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
		chunkCopies = append(chunkCopies, c.hanleDataWrite((nodeID+of1)%len(c.nodes), dataChannel))
		chunkCopies = append(chunkCopies, c.hanleDataWrite((nodeID+of2)%len(c.nodes), dataChannel))
		entry.Write(nodeID, chunkCopies)
	}

	c.updateFileEntry(entry)
}

func (c *ChunkServer) updateFileEntry(entry FileEntry) {
	var cmd = &Message{Command: "updateFileEntry", Args: []string{entry.GetName()}}
	conn, err := net.Dial("tcp", ":"+os.Getenv("META_SERVER_PORT"))
	defer conn.Close()
	if err != nil {
		log.Println(err.Error())
	}
	enc := gob.NewEncoder(conn)
	err = enc.Encode(cmd)
	if err != nil {
		log.Println(err.Error())
	}
	var msg struct {
		Entry *File
	}

	msg.Entry, _ = entry.(*File)
	gob.Register(msg.Entry.Chunks[0])
	err = enc.Encode(msg)
	if err != nil {
		log.Println(err.Error())
	}
}

func (c *ChunkServer) handleKillConnection(nodeID int) {
	var rmsg struct {
		Result string
		Err    string
	}
	node := c.nodes[nodeID]
	node.Kill()
	rmsg.Result = fmt.Sprintf("node with id %d successfully killed", nodeID)
	_ = c.encoder.Encode(rmsg)
}

func (c *ChunkServer) hanleDataWrite(nodeID int, dataChannel <-chan []byte) Copy {
	node := c.nodes[nodeID]
	addr, size := node.Write(dataChannel)
	return Copy{Node: nodeID, Addr: addr, Valid: true, Size: size}
}

func (c *ChunkServer) computeReplica(nodeID int) (int, int) {

	offset1 := 1
	offset2 := c.NODEPERRACK - 1
	if (nodeID+offset1) >= len(c.nodes) && (nodeID-offset1) >= 0 {
		offset1 = -offset1
	}
	if (nodeID+offset2) >= len(c.nodes) && (offset2-nodeID) >= 0 {
		offset2 = offset2 - nodeID
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
	var err error
	n.mutex.Lock()
	if len(n.content) > addr {
		n.content[addr] = n.content[len(n.content)-1]
		n.content = n.content[:len(n.content)-1]
		err = nil
	} else {
		err = fmt.Errorf("invalid chunk address at %d", addr)
	}
	n.mutex.Unlock()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (n *Node) Write(dataChannel <-chan []byte) (int, int) {
	data := <-dataChannel
	var chunk ChunkFile
	chunk.Write(data)
	n.content = append(n.content, &chunk)
	return len(n.content) - 1, cap(data)
}

func (n *Node) IsRunning() bool {
	return !n.isKilled
}
