# PyPregel #
Final project for 2020 Spring CS525 Parallel Computing.

Team members:
* Chufeng Gao
* Hongxin Chu
* Zichen Wang

PyPregel is a parallel system for large scale graph computing in python based on [Pregel](https://kowshik.github.io/JPregel/pregel_paper.pdf). It is designed for programs that are expressed as a sequence of iterations.

At a iteration, each vertex can:
* Receive messages sent in the previous iteration
* Send messages to other vertices
* Modify its own state (vertex value, outgoing edges)
 
We call a iteration as `Superstep` in PyPregel.


### Installation ###
On the parent directory of pypregel do:
`pip install -e pypregel`

### Run ###

`config.txt`: configuration input file. Include number of vertices.

`graph.txt`: input graph. User implemented reader will read in this file.

````
mpirun -np [# of processes] python3 APPLICATION CONFIG_FILE GRAPH_FILE OUTPUT_FILE
````

### Example
There are 2 built-in examples for pypregel. PageRank and Single Source Shortest Path.

#### PageRank
After [installation](#installation) of pregel package, inside apps/pagerank, do:
````
mpirun -np 4 python3 pagerank.py config.txt graph.txt output.txt
````

#### Single Source Shortest Path
After [installation](#installation) of pregel package, inside apps/sssp, do:
````
mpirun -np 4 python3 sssp.py config.txt graph.txt output.txt
````