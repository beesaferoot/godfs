package server

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"sync"
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
	Delete()
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
	socket      net.Listener
	CHUNKSIZE   int
	NODEPERRACK int
	RACKNUMBER  int
	data        string
	nodes       []Node
	PORT        int
}

type DataNode interface {
	GetSize() int
	Run()
	Write(int, []byte)
	Kill(bool)
	Read(int) string
	Delete(int) (bool, error)
}

type Node struct {
	id       int
	size     int
	count    int
	isKilled bool
	content  []Chunk
	mutex    sync.RWMutex
}

func (c *Copy) stopNode() {
	c.valid = false
}

func (c ChunkFile) Read() []byte {
	return c.data
}

func (c ChunkFile) Write(fragment []byte) {
	c.data = fragment
}

func (c ChunkFile) Delete() {
	c.data = make([]byte, c.GetMaxSize())
}

func (c ChunkFile) GetMaxSize() int {
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
	return c.copies, nil

}

func (c *ChunkServer) sendMsg(msg Message) error {
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
