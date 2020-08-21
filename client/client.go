package client

import (
	"encoding/gob"
	"fmt"
	"goSimDFS/server"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strconv"
)

type Message struct {
	Result string
	Err    string
}
type FileSystem interface {
	Read(string) string
	Write(string, io.Reader)
	GetDiskCapacity()
	Rename(string, string)
	ListFiles()
	GetFileSize(string)
	GetFileStat(string)
	GetNodeStat()
	GetNodeStatById(int)
	StopNode()
	Kill()
}

type Socket struct {
	encoder *gob.Encoder
	decoder *gob.Decoder
}

type Client struct {
	metaServerSocket  Socket
	chunkServerSocket Socket
}

func NewClient(metaconn, chunkconn net.Conn) FileSystem {
	menc := gob.NewEncoder(metaconn)
	mdec := gob.NewDecoder(metaconn)
	cenc := gob.NewEncoder(chunkconn)
	cdec := gob.NewDecoder(chunkconn)
	return &Client{metaServerSocket: Socket{encoder: menc, decoder: mdec},
		chunkServerSocket: Socket{encoder: cenc, decoder: cdec}}
}

func (c *Client) Kill() {
	var cmd = server.Message{Command: "killserver"}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Fatal("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}
}

func (c *Client) Read(filename string) string {
	var fileString string
	var cmd = server.Message{Command: "read", Args: []string{filename}}
	var rmsg struct {
		Result *server.File
		Err    string
	}
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		chunk := &server.ChunkMetadata{}
		gob.Register(chunk)
	}
	err = c.metaServerSocket.decoder.Decode(&rmsg)
	if err != nil {
		if err == io.EOF {
		} else {
			log.Fatal("decode error: ", err.Error())
		}
	}
	if len(rmsg.Err) > 0 {
		log.Println(rmsg.Err)
	} else {
		err = c.chunkServerSocket.encoder.Encode(&cmd)
		if err != nil {
			log.Println(err.Error())
		} else {
			err = c.chunkServerSocket.encoder.Encode(rmsg.Result)
			if err != nil {
				log.Println(err.Error())
			} else {
				err = c.chunkServerSocket.decoder.Decode(&fileString)
				if err != nil {
					if err == io.EOF {
					} else {
						log.Fatal("decode error: ", err.Error())
					}
				}
				return fileString
			}

		}
	}
	return fileString
}

func (c *Client) Write(filename string, file io.Reader) {
	var buf []byte
	buf, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal(err.Error())
	}
	var cmd = server.Message{Command: "write", Args: []string{filename, strconv.Itoa(len(buf))}}
	var rmsg struct {
		Result *server.File
		Err    string
	}

	err = c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		chunk := &server.ChunkMetadata{}
		gob.Register(chunk)
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Fatal("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			err = c.chunkServerSocket.encoder.Encode(&cmd)
			if err != nil {
				log.Println(err.Error())
			} else {

				err = c.chunkServerSocket.encoder.Encode(rmsg.Result)
				if err != nil {
					log.Println(err.Error())
				} else {
					err = c.chunkServerSocket.encoder.Encode(buf)
					if err != nil {
						log.Println(err.Error())
					}
				}
			}
		}
	}
}

func (c *Client) Rename(old string, new string) {
	var cmd = server.Message{Command: "rename", Args: []string{old, new}}
	var rmsg struct {
		Err string
	}
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Fatal("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println("file successfully renamed")
		}
	}
}

func (c *Client) ListFiles() {
	var cmd = server.Message{Command: "ls"}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Fatal("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}
}

func (c *Client) GetDiskCapacity() {
	var cmd = server.Message{Command: "diskcapacity"}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Fatal("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}

}

func (c *Client) GetFileSize(filename string) {
	var cmd = server.Message{Command: "filesize", Args: []string{filename}}
	var rmsg struct {
		Result int
		Err    string
	}
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Fatalln(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Println("decode error: ", err.Error())
			}
		}
		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			if rmsg.Result < 0 {
				fmt.Println(filename, " does not exists. ")
			} else {
				fmt.Println(rmsg.Result, " bytes")
			}

		}
	}

}

func (c *Client) GetFileStat(filename string) {
	var cmd = server.Message{Command: "stat", Args: []string{filename}}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Println("decode error: ", err.Error())
			}
		}

		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}

}

func (c *Client) GetNodeStat() {
	var cmd = server.Message{Command: "nodestat"}
	var rmsg Message
	err := c.chunkServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.chunkServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Println("decode error: ", err.Error())
			}
		}

		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}

}

func (c *Client) GetNodeStatById(nodeID int) {
	var cmd = server.Message{Command: "nodestat", Args: []string{strconv.Itoa(nodeID)}}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {
		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Println("decode error: ", err.Error())
			}
		}

		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			fmt.Println(rmsg.Result)
		}
	}
}

func (c *Client) StopNode() {
	var cmd = server.Message{Command: "stopnode"}
	var rmsg Message
	err := c.metaServerSocket.encoder.Encode(&cmd)
	if err != nil {
		log.Println(err.Error())
	} else {

		err = c.metaServerSocket.decoder.Decode(&rmsg)
		if err != nil {
			if err == io.EOF {
			} else {
				log.Println("decode error: ", err.Error())
			}
		}

		if len(rmsg.Err) > 0 {
			log.Println(rmsg.Err)
		} else {
			cmd.Command = "killnode"
			cmd.Args = []string{rmsg.Result}
			err = c.chunkServerSocket.encoder.Encode(&cmd)
			if err != nil {
				log.Println(err.Error())
			}
			err = c.chunkServerSocket.decoder.Decode(&rmsg)
			if err != nil {
				if err == io.EOF {
				} else {
					log.Println("decode error: ", err.Error())
				}
			}
			if len(rmsg.Err) > 0 {
				log.Println(rmsg.Err)
			} else {
				fmt.Println(rmsg.Result)
			}
		}
	}

}
