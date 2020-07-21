package server

import (
	"net"
)

type Chunk struct {
	data  []byte
	valid bool
}

type ChunkEntry struct {
	index  int
	copies []Copy
}

func (c *ChunkEntry) stopNode(int) {

}

type ChunkServer struct {
	socket      net.Dialer
	CHUNKSIZE   int
	NODEPERRACK int
	RACKNUMBER  int
	data        string
	nodes       []Node
}

type Node struct {
	id       int
	size     int
	count    int
	isKilled bool
	content  Chunk
}
