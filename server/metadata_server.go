package server

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

type Copy struct {
	node  int
	addr  int
	valid bool
}	

type FileEntry struct {
	name        string
	size        int
	createdDate time.Time
	chunks      ChunkEntry
}

func (f *FileEntry) rename(newFileName string ){

}

type MetaServer interface {
	rename(string, string) error
	fileSize(string) int
	fileStat(string) (string, error)
	read(string, string)
	write(string)
	stopNode(int)
	getDiskCap() int
	sendMsg(string) error
	updateDiskCap()
	nodeStat() string
}

type MasterNode struct {
	socket            net.Listener
	serverName        string
	diskCap           int
	CHUNKSIZE         int
	ROW               int
	COLUMN            int
	nodeMap           [][]int
	CHUNK_SERVER_PORT int
	files             []FileEntry
}

func (m *MasterNode) sendMsg(msg string) (int, error) {
	conn, err := net.Dial("tcp", string(m.CHUNK_SERVER_PORT))
	defer conn.Close()
	if err != nil {
		return -1, err
	}
	// var packet [] byte
	buf := bytes.NewBufferString(msg)
	n, err := conn.Write(buf.Bytes())
	if err != nil {
		return -1, fmt.Errorf("Error: could not accept incomming request: %v", err.Error())
	}

	return n, nil

}

func (m *MasterNode) fileSize(filename string) int {
	// return file entry size with the specified filename or return -1 if entry is non-existent
	for _, entry := range m.files {
		if entry.name == filename {
			return entry.size
		}
	}
	return -1
}

func (m *MasterNode) rename(oldFileName string, newFileName string) error {

	for _, entry := range m.files {
		if entry.name == newFileName {
			entry.rename(newFileName)
		}
	}

	return fmt.Errorf("%v does not exist ", oldFileName)

}

func (m *MasterNode) getDiskCap() int {
	
	return m.diskCap
}

func (m *MasterNode) updateDiskCap(){
	var totalDiskCap int
	for _, node := range m.nodeMap{
		totalDiskCap += node[1]
	}
	m.diskCap = totalDiskCap
}

func (m *MasterNode) fileStat(filename string) (string, error){
	
	for _, entry := range m.files {
		if entry.name == filename {
			return fmt.Sprintf(
				`file name: \t%#v
				 created: \t%#v
				 size: \t%#v`, entry.name, entry.createdDate, entry.size), nil
		}
	}
	return "", fmt.Errorf("file does not exist")
}

func (m *MasterNode) nodeStat() string{

	var statString string
	statString = fmt.Sprintf("TotalDiskSpace: %#v", m.diskCap)
	for idx, node := range m.nodeMap{
		statString += fmt.Sprintf("Node_%v available space: %#v", idx+1, node[2])
	}
	return statString
}


func (m *MasterNode) stopNode(nodeId int ) error {

	if nodeId < 0 || nodeId > m.ROW{
		return fmt.Errorf("invalid node ID")
	}

	for _, entry := range m.files {
		entry.chunks.stopNode(nodeId)
	}
	return	 nil
}