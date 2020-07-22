package server

import (
	"fmt"
	"io"
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
	sync.Locker
	Read() string
	Write([]byte)
	Delete()
}

type ChunkFile struct {
	data  []byte
	valid bool
}

func (c ChunkFile) Read() string {
	return string(c.data)
}

func (c ChunkFile) Write(fragment []byte) {
	c.data = fragment
}

type ChunkEntry interface {
	Read() (string, error)
	stopNode(int)
}

type ChunkMetadata struct {
	index  int
	copies []Copy
}

func (c *ChunkMetadata) stopNode(int) {

}

func (c *ChunkMetadata) Read() (string, error) {
	conn, err := net.Dial("tcp", os.Getenv("CHUNK_SERVER_PORT"))
	if err != nil {
		return "", fmt.Errorf("Network Error: %v", err.Error())
	}
	defer conn.Close()
	var filechunk []byte

	conn.Write([]byte(fmt.Sprintf("READNODE-%d",c.index)))
	for {
		n, err := conn.Read(filechunk)
		if err != nil {
			if err == io.EOF {
				return string(filechunk[:n]), nil
			}
			return "", fmt.Errorf("%v", err.Error())
		}

	}

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
	GetSize()
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
	content  Chunk
}
