# goSimDFS
Distributed File System Client Simulator 

## Design Architecture Guide 
This implementation is based on the research paper [DFS_simulation_14m.pdf](http://www.cse.scu.edu/~mwang2/projects/DFS_simulation_14m.pdf "A simulation of distributed file system" )


## Simulation structure 
![](Screenshot%20from%202020-07-21%2012-14-55.png)

## Implementation differences 
While the design is based on the above paper, this implementation leverages concurrency benefits in the [go programming language](https://tour.golang.org/list).

## Usage 
    usage: ./goSimDFS [help] <command> [<args>]

  - export environment variables
   - `` export META_SERVER_PORT=$(PORT)``
   - `` export CHUNK_SERVER_PORT=$(PORT)``

  - start filesystem servers
    - `` ./goSimDFS start ``

  - run commands 
    - `` ./goSimDFS <command> [args]``

## Build 
    go build 

## Requirement 
    this project requires a unix-like shell to work properly.
## Contributions
- are you interested in distributed systems? 
- or want to learn about file systems? 
- testing of modules.
Contributions are welcomed, hope to expand on this project. 
