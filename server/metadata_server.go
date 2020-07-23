package server

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// Generic Message for client/server communication
type Message struct {
	Command string
	Args    []string
}

type FileEntry interface {
	Read() []ChunkEntry
	Rename(string)
	GetSize() int
	StopNode(int)
	Write(int, []Copy)
	Name() string
	Date() time.Time
	getChunks() []ChunkEntry
}

type File struct {
	name        string
	size        int
	createdDate time.Time
	chunks      []ChunkEntry
}

type MetaServer interface {
	Rename(string, string) error
	FileSize(string) int
	FileStat(string) (string, error)
	Read(string) ([]ChunkEntry, error)
	Write(string) FileEntry
	stopNode() int
	GetDiskCap() int
	sendMsg(Message) error
	UpdateDiskCap()
	GetNodeStat(interface{}) string
	nodeStatByID(int) string
	nodeStat() string
	Run()
}

type MasterNode struct {
	socket     net.Listener
	serverName string
	diskCap    int
	CHUNKSIZE  int
	ROW        int
	COLUMN     int
	nodeMap    [4][2]int //predefined datastore nodes to be used
	files      map[string]FileEntry
	PORT       int
}

func (f *File) Rename(newFileName string) {
	f.name = newFileName
}

func (f *File) GetSize() int {
	return f.size
}

func (f *File) StopNode(nodeID int) {
	for _, entry := range f.chunks {
		entry.stopNode(nodeID)
	}
}

func (f *File) Write(nodeID int, copies []Copy) {
	var chunkEntry ChunkMetadata
	chunkEntry.index = nodeID
	chunkEntry.copies = copies
	f.chunks = append(f.chunks, &chunkEntry)
}

func (f *File) Read() []ChunkEntry {
	
	return getChunks() 
}

func (f *File) Date() time.Time {
	return f.createdDate
}

func (f *File) Name() string {
	return f.name
}

func (f *File) GetChunks() []ChunkEntry {
	return f.chunks
}

func NewMasterNode(serverName string, serverConfig map[string]interface{}) *MasterNode {
	var DefaultConfig = map[string]int{}
	DefaultConfig["CHUNKSIZE"] = 100
	DefaultConfig["PORT"] = 5000
	DefaultConfig["NODES"] = 4
	var newMasterNode = MasterNode{serverName: serverName}

	if val, ok := serverConfig["PORT"]; ok {
		if port, ok := val.(int); ok {
			newMasterNode.PORT = port
		} else {
			log.Fatal("invalid type for server port value, expected an integer")
		}
	} else {
		fmt.Printf("using default port: %d\n", DefaultConfig["PORT"])
		newMasterNode.PORT = DefaultConfig["PORT"]
	}

	if val, ok := serverConfig["CHUNKSIZE"]; ok {
		if chunkSize, ok := val.(int); ok {
			newMasterNode.CHUNKSIZE = chunkSize
		} else {
			log.Fatal("invalid type for chunksize value, expected an integer")
		}
	} else {
		fmt.Printf("using default chunksize: %d\n", DefaultConfig["CHUNKSIZE"])
		newMasterNode.CHUNKSIZE = DefaultConfig["CHUNKSIZE"]
	}

	for _, node := range newMasterNode.nodeMap {
		node[1] = newMasterNode.CHUNKSIZE
	}
	newMasterNode.files = map[string]FileEntry{}
	return &newMasterNode

}

func (m *MasterNode) sendMsg(msg Message) error {
	conn, err := net.Dial("tcp", fmt.Sprintf(":%s", os.Getenv("CHUNK_SERVER_PORT")))
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

func (m *MasterNode) FileSize(filename string) int {
	// return file entry size with the specified filename or return -1 if entry is non-existent
	if entry, ok := m.files[filename]; ok {
		return entry.GetSize()
	}
	return -1
}

func (m *MasterNode) Rename(oldFileName string, newFileName string) error {

	if entry, ok := m.files[oldFileName]; ok {
		entry.Rename(newFileName)
		return nil
	}

	return fmt.Errorf("%s does not exist ", oldFileName)

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

	if entry, ok := m.files[filename]; ok {
		return fmt.Sprintf(
			`file name: \t%s
			 created: \t%v
			 size: \t%d`, entry.Name(), entry.Date(), entry.GetSize()), nil
	}
	return "", fmt.Errorf("file does not exist")
}

func (m *MasterNode) GetNodeStat(param interface{}) string {

	switch x := param.(type) {
	case int:
		return m.nodeStatByID(x)
	default:
		return m.nodeStat()
	}
}

func (m *MasterNode) nodeStatByID(nodeID int) string {
	var statString string
	if nodeID > -1 && nodeID < m.ROW {
		statString = fmt.Sprintf("node %d available space: %d", nodeID, m.nodeMap[nodeID][1])
	}
	statString = fmt.Sprintf("no Node with ID %d", nodeID)
	return statString
}

func (m *MasterNode) nodeStat() string {

	var statString string
	statString = fmt.Sprintf("totaldiskspace: %d", m.diskCap)
	for idx, node := range m.nodeMap {
		statString += fmt.Sprintf("node %d available space: %d", idx+1, node[1])
	}
	return statString
}

func (m *MasterNode) stopNode() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(m.ROW - 1)
}

func (m *MasterNode) Read(fileName string) ([]ChunkEntry, error) {
	if entry, ok := m.files[fileName]; ok {
		return entry.Read(), nil
	}
	return nil, fmt.Errorf("file does not exist")
}

func (m *MasterNode) Write(filename string) FileEntry {
	// if file exists return file entry else create a new entry using filename
	if entry, ok := m.files[filename]; ok {
		return entry
	}
	var entry FileEntry = &File{name: filename}
	return entry

}

func (m *MasterNode) Run() {
	var err error
	m.socket, err = net.Listen("tcp", fmt.Sprintf(":%d", m.PORT))
	if err != nil {
		log.Fatalf("unable to start %s server: %v\n", m.serverName, err.Error())
	}
	fmt.Printf("starting %v server at port %d\n", m.serverName, m.PORT)
	m.UpdateDiskCap()
	fmt.Printf("listening on port %d\n", m.PORT)

	for {
		conn, err := m.socket.Accept()
		if err != nil {
			log.Fatal(err.Error())
		}
		m.handleConnection(conn)

	}

}

func (m *MasterNode) handleConnection(conn net.Conn) {
	buf := make([]byte, m.CHUNKSIZE)
	defer conn.Close()
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err.Error())
		}
		fmt.Println(string(buf[:n]))
	}
}
