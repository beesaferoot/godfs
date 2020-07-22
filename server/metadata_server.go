package server

import (
	"fmt"
	"net"
	"os"
	"time"
)

type FileEntry interface {
	Read() (string, error)
	Rename(string)
	GetSize() int
	StopNode(int)
	Write(int, []Copy)
	Name() string
	Date() time.Time
	GetChunks() ChunkEntry
}

type File struct {
	name        string
	size        int
	createdDate time.Time
	chunks      []ChunkEntry
}

func (f *File) Rename(newFileName string) {

}

func (f *File) GetSize() int {
	return f.size
}

func (f *File) StopNode(nodeID int) {

}

func (f *File) Write(nodeID int, copies []Copy) {
	var chunkEntry ChunkMetadata
	chunkEntry.index = nodeID
	chunkEntry.copies = copies
	f.chunks = append(f.chunks, &chunkEntry)
}

func (f *File) Read() (string, error) {
	var fileContent string
	for _, entry := range f.chunks {
		
		_chunk, err := entry.Read()
		if err != nil {
			return "", fmt.Errorf("%v", err.Error())
		}
		fileContent += _chunk
	}
	return fileContent, nil
}

type MetaServer interface {
	Rename(string, string) error
	FileSize(string) int
	FileStat(string) (string, error)
	Read(string, string) string
	Write(string)
	stopNode(int)
	GetDiskCap() int
	sendMsg(string) error
	UpdateDiskCap()
	nodeStat() string
}

type MasterNode struct {
	socket     net.Listener
	serverName string
	diskCap    int
	CHUNKSIZE  int
	ROW        int
	COLUMN     int
	nodeMap    [][]int
	files      []FileEntry
	PORT       int
}

func (m *MasterNode) sendMsg(msg string) (int, error) {
	conn, err := net.Dial("tcp", os.Getenv("CHUNK_SERVER_PORT"))
	defer conn.Close()
	if err != nil {
		return -1, err
	}

	n, err := conn.Write([]byte(msg))
	if err != nil {
		return -1, fmt.Errorf("Error: could not accept incomming request: %v", err.Error())
	}

	return n, nil

}

func (m *MasterNode) fileSize(filename string) int {
	// return file entry size with the specified filename or return -1 if entry is non-existent
	for _, entry := range m.files {
		if entry.Name() == filename {
			return entry.GetSize()
		}
	}
	return -1
}

func (m *MasterNode) Rename(oldFileName string, newFileName string) error {

	for _, entry := range m.files {
		if entry.Name() == newFileName {
			entry.Rename(newFileName)
		}
	}

	return fmt.Errorf("%v does not exist ", oldFileName)

}

func (m *MasterNode) GetDiskCap() int {

	return m.diskCap
}

func (m *MasterNode) UpdateDiskCap() {
	var totalDiskCap int
	for _, node := range m.nodeMap {
		totalDiskCap += node[1]
	}
	m.diskCap = totalDiskCap
}

func (m *MasterNode) FileStat(filename string) (string, error) {

	for _, entry := range m.files {
		if entry.Name() == filename {
			return fmt.Sprintf(
				`file name: \t%#v
				 created: \t%#v
				 size: \t%#v`, entry.Name(), entry.Date(), entry.GetSize()), nil
		}
	}
	return "", fmt.Errorf("file does not exist")
}

func (m *MasterNode) nodeStat() string {

	var statString string
	statString = fmt.Sprintf("TotalDiskSpace: %#v", m.diskCap)
	for idx, node := range m.nodeMap {
		statString += fmt.Sprintf("Node_%v available space: %#v", idx+1, node[2])
	}
	return statString
}

func (m *MasterNode) stopNode(nodeID int) error {

	if nodeID < 0 || nodeID > m.ROW {
		return fmt.Errorf("invalid node ID")
	}

	for _, entry := range m.files {
		entry.GetChunks().stopNode(nodeID)
	}
	return nil
}
