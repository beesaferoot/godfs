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
	GetName() string
	Date() time.Time
	getChunks() []ChunkEntry
	DeleteChunks()
}

type File struct {
	Name        string
	Size        int
	CreatedDate time.Time
	Chunks      []ChunkEntry
}

type MetaServer interface {
	Rename(string, string) error
	FileSize(string) int
	FileStat(string) (string, error)
	ListFiles() string
	Read(string) (FileEntry, error)
	Write(string) FileEntry
	stopNode() int
	GetDiskCap() int
	sendMsg(*Message) error
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
	nodeMap    []int
	files      map[string]FileEntry
	PORT       int
	encoder    *gob.Encoder
	decoder    *gob.Decoder
}

func (f *File) Rename(newFileName string) {
	f.Name = newFileName
}

func (f *File) GetSize() int {
	return f.Size
}

func (f *File) StopNode(nodeID int) {
	for _, entry := range f.Chunks {
		entry.stopNode(nodeID)
	}
}

func (f *File) Write(nodeID int, copies []Copy) {
	var chunkEntry ChunkMetadata
	chunkEntry.Index = nodeID
	chunkEntry.Copies = copies
	f.Chunks = append(f.Chunks, &chunkEntry)
}

func (f *File) Read() []ChunkEntry {
	return f.getChunks()
}

func (f *File) DeleteChunks() {
	f.Chunks = f.Chunks[:0]
}

func (f *File) Date() time.Time {
	return f.CreatedDate
}

func (f *File) GetName() string {
	return f.Name
}

func (f *File) getChunks() []ChunkEntry {
	return f.Chunks
}

func NewMasterServer(serverName string, serverConfig map[string]interface{}) *MasterNode {
	const DEFAULT_ALLOCATED_DISKSPACE = 4000
	var DefaultConfig = map[string]int{}
	DefaultConfig["chunksize"] = 100
	DefaultConfig["nodes"] = 4
	var newMasterNode = MasterNode{serverName: serverName, COLUMN: 4}

	if val, ok := serverConfig["port"]; ok {
		if port, ok := val.(int); ok {
			newMasterNode.PORT = port
		} else {
			log.Fatalln("invalid type for server port value, expected an integer")
		}
	} else {
		fmt.Printf("using default port: %d\n", DefaultConfig["port"])
		newMasterNode.PORT = DefaultConfig["port"]
	}

	if val, ok := serverConfig["chunksize"]; ok {
		if chunkSize, ok := val.(int); ok {
			newMasterNode.CHUNKSIZE = chunkSize
		} else {
			log.Fatalln("invalid type for chunksize value, expected an integer")
		}
	} else {
		fmt.Printf("using default chunksize: %d\n", DefaultConfig["chunksize"])
		newMasterNode.CHUNKSIZE = DefaultConfig["chunksize"]
	}

	if val, ok := serverConfig["nodes"]; ok {
		if totalnodes, ok := val.(int); ok {
			newMasterNode.ROW = totalnodes
		} else {
			log.Fatalln("invalid type for nodes value, expected an interger")
		}
	} else {
		fmt.Printf("using default nodes: %d\n", DefaultConfig["nodes"])
		newMasterNode.ROW = DefaultConfig["nodes"]
	}

	for i := 0; i < newMasterNode.ROW; i++ {
		newMasterNode.nodeMap = append(newMasterNode.nodeMap, DEFAULT_ALLOCATED_DISKSPACE)
	}
	newMasterNode.files = map[string]FileEntry{}
	return &newMasterNode

}

func (m *MasterNode) ListFiles() string {
	var fileString string
	if len(m.files) == 0 {
		return fileString
	}
	for _, entry := range m.files {
		fileString += fmt.Sprintf("%s  ", entry.GetName())
	}
	return fileString
}
func (m *MasterNode) sendMsg(msg *Message) error {
	conn, err := net.Dial("tcp", ":"+os.Getenv("CHUNK_SERVER_PORT"))
	defer conn.Close()
	if err != nil {
		return err
	}
	m.encoder = gob.NewEncoder(conn)
	err = m.encoder.Encode(msg)
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
		m.files[newFileName] = entry
		delete(m.files, oldFileName)
		return nil
	}

	return fmt.Errorf("%s does not exist", oldFileName)

}

func (m *MasterNode) GetDiskCap() int {

	return m.diskCap
}

func (m *MasterNode) UpdateDiskCap() {
	var totalDiskCap int
	for _, spaceLeft := range m.nodeMap {
		totalDiskCap += spaceLeft
	}
	m.diskCap = totalDiskCap
}

func (m *MasterNode) FileStat(filename string) (string, error) {

	if entry, ok := m.files[filename]; ok {
		return fmt.Sprintf(
			`file name:   %s
             created:     %v
             size:        %d bytes`, entry.GetName(), entry.Date(), entry.GetSize()), nil
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
		statString = fmt.Sprintf("node %d available space: %d", nodeID, m.nodeMap[nodeID])
	} else {
		statString = fmt.Sprintf("no Node with ID %d", nodeID)
	}
	return statString
}

func (m *MasterNode) nodeStat() string {

	var statString string
	statString = fmt.Sprintf("totaldiskspace: %d", m.diskCap)
	for idx, spaceLeft := range m.nodeMap {
		statString += fmt.Sprintf("node %d available space: %d\n", idx+1, spaceLeft)
	}
	return statString
}

func (m *MasterNode) stopNode() int {
	rand.Seed(time.Now().UnixNano())
	nodeID := rand.Intn(m.ROW - 1)
	for _, entries := range m.files {
		entries.StopNode(nodeID)
	}
	return nodeID
}

func (m *MasterNode) Read(fileName string) (FileEntry, error) {
	if entry, ok := m.files[fileName]; ok {
		return entry, nil
	}
	return nil, fmt.Errorf("file does not exist")
}

func (m *MasterNode) Write(filename string) FileEntry {
	// if file exists return file entry else create a new entry using filename
	if entry, ok := m.files[filename]; ok {
		return entry
	}
	entry := &File{Name: filename}
	m.files[entry.GetName()] = entry
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

	chunkServerConfig := map[string]interface{}{
		"port":        os.Getenv("CHUNK_SERVER_PORT"),
		"nodes":       m.ROW,
		"chunksize":   m.CHUNKSIZE,
		"NO_PER_RACK": m.COLUMN,
	}
	chunkServer := NewChunkServer("chunk", chunkServerConfig)
	go chunkServer.Run()

	for {
		conn, err := m.socket.Accept()
		if err != nil {
			log.Fatal(err.Error())
		}
		m.encoder = gob.NewEncoder(conn)
		m.decoder = gob.NewDecoder(conn)
		go m.handleConnection(conn)

	}

}

func (m *MasterNode) handleConnection(conn net.Conn) {

	defer conn.Close()
	var msg Message
	var err error
	for {
		err = m.decoder.Decode(&msg)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("decode error: ", err.Error())
			m.encoder.Encode(err.Error())
			break
		}
		if msg.Command == "killserver" {
			// end process
			var rmsg struct {
				Result string
				Err    string
			}

			rmsg.Result = "servers stopped running"
			err = m.encoder.Encode(rmsg)
			if err != nil {
				log.Println(err.Error())
			}
			os.Exit(1)
		}
		m.handleClientCommands(&msg)

	}

}

func (m *MasterNode) handleClientCommands(msg *Message) {
	switch msg.Command {
	case "stopnode":
		var rmsg struct {
			Result string
			Err    string
		}
		rmsg.Result = strconv.Itoa(m.stopNode())
		_ = m.encoder.Encode(rmsg)
		break
	case "stat":
		stat, err := m.FileStat(msg.Args[0])
		var rmsg struct {
			Result string
			Err    string
		}

		if err != nil {
			rmsg.Err = err.Error()
			_ = m.encoder.Encode(rmsg)
			break
		}
		rmsg.Result = stat
		err = m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	case "nodestat":
		var rmsg struct {
			Result string
			Err    string
		}
		nodeID, err := strconv.Atoi(msg.Args[0])
		if err != nil {
			rmsg.Err = err.Error()
		} else {
			rmsg.Result = m.GetNodeStat(nodeID)
		}
		err = m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
	case "ls":
		var rmsg struct {
			Result string
			Err    string
		}
		rmsg.Result = m.ListFiles()
		err := m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	case "diskcapacity":
		var rmsg struct {
			Result string
			Err    string
		}
		rmsg.Result = fmt.Sprintf("total diskcapacity: %d", m.GetDiskCap())
		_ = m.encoder.Encode(rmsg)
		break
	case "rename":
		var rmsg struct {
			Err string
		}
		err := m.Rename(msg.Args[0], msg.Args[1])
		if err != nil {
			rmsg.Err = err.Error()
		}
		err = m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err)
		}
		break
	case "read":
		var rmsg struct {
			Result *File
			Err    string
		}
		filename := msg.Args[0]
		entry, err := m.Read(filename)
		if err != nil {
			rmsg.Err = err.Error()
		} else {
			rmsg.Result = entry.(*File)
		}
		err = m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	case "write":
		var rmsg struct {
			Result *File
			Err    string
		}
		filename := msg.Args[0]
		filesize, _ := strconv.Atoi(msg.Args[0])
		if filesize >= m.GetDiskCap() {
			rmsg.Err = "not enough availabe disk space for file"
		} else {
			entry := m.Write(filename)
			rmsg.Result = entry.(*File)
		}
		err := m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	case "filesize":
		var rmsg struct {
			Result int
			Err    string
		}
		filename := msg.Args[0]
		rmsg.Result = m.FileSize(filename)
		err := m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	case "updateFileEntry":
		var rmsg struct {
			Entry *File
		}
		err := m.decoder.Decode(&rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		if entry, ok := m.files[msg.Args[0]]; ok {
			if len(entry.getChunks()) == 0 {
				for _, chunk := range rmsg.Entry.getChunks() {
					m.nodeMap[chunk.Id()] = m.nodeMap[chunk.Id()] - chunk.Size()
				}
			}
		}
		rmsg.Entry.Size = rmsg.Entry.Chunks[0].Size()
		m.files[msg.Args[0]] = rmsg.Entry
		m.UpdateDiskCap()
		break
	default:
		var rmsg struct {
			Err string
		}
		rmsg.Err = fmt.Sprintf("%s is not a valid command", msg.Command)
		err := m.encoder.Encode(rmsg)
		if err != nil {
			log.Println(err.Error())
		}
		break
	}
}
