# Task 2 - Distributed key-value store (KVS)

In this task you will extend the single-node key-value store (KVS) to multiple
nodes by sharding (partitioning) keys.

## Architecture of the distributed key-value store

We will rely on a central coordination service for the distributed key-value
store that keeps track which key belongs to which node. The central
coordination service in this task will be the routing tier (simply called
router), i.e., a separate node that forwards client requests to the correct
node and that manages joining and leaving nodes in our cluster of nodes. For
sharding we will use a simple but effective approach: we will split the
key-space into a fixed number of partitions with considerably more partitions
than nodes. When the first node joins the cluster, all partitions (e.g., 1000
partitions) are created on this node. Subsequent nodes then steal whole
partitions from existing nodes until partitions are fairly distributed. In such
manner, neither the number of partitions changes nor the assignment of keys to
partitions, only the assignment of partitions to nodes changes. This is a
common approach used by major database systems, e.g., Voldemort and Couchbase.

## Router

The router receives GET, PUT and DELETE requests from the controller. Requests
arrive at the API port and are forwarded to the P2P port of the node
responsible for the key. Additionally, the router receives JOIN_CLUSTER
requests from new nodes. Whenever a new node joins, the router updates the
mapping of partitions to nodes and instructs the new nodes to steal partitions
from existing nodes. Whenever partitions are added or removed from nodes, nodes
inform the router about the changes in partition mapping.

The router can be run like this:

```
./build/router-test -a 127.0.0.1:40000 -r 127.0.0.1:41000
```

## KV store

The kv store can be used like this, where the last argument is the cluster address which is the routing address in this case:

```
./build/kvs-test -a 127.0.0.1:42000 -p 127.0.0.1:43000 -c 127.0.0.1:41000
```

## Controller

The controller submits GET, PUT and DELETE requests to the API port of the
key-value server. Additionally, it submits JOIN_CLUSTER requests to nodes to
join a cluster of nodes (which in turn send a JOIN_CLUSTER request to the
routing tier). The code for the controller is provided in the source directory.

The controller can be run like this:

```
./build/ctl-test -a 127.0.0.1:40000 join 127.0.0.1:43000
./build/ctl-test -a 127.0.0.1:40000 put 5 2
./build/ctl-test -a 127.0.0.1:40000 get 5
./build/ctl-test -a 127.0.0.1:40000 del 5
```

## Tasks

Your task is to implement the functions that have annotated as: 

```TODO (you)```

Additionally, you must include the following messages for the test case to pass:

- Key and value for a get request, value should be **ERROR** in case the key doesn't exist
- **OK** for a successfull join request

### Task 2.1 - Router

Your task is to implement the routing tier such that new nodes can join the
cluster and partitions are fairly distributed onto the nodes. You do not need
to handle leaving (or failing) nodes.

### Task 2.2 - Server

Implement the partition re-distribution mechanism on the server, i.e., request
another node for a partition, add it on the current node and inform the routing
tier about added/removed partitions.

### Task 2.3 - Failure detection

Implement a failure detection mechanism in the router that checks if servers have crashed.

## Build instructions

1. Install all required packages:

   ```
   sudo apt-get install build-essential cmake libgtest-dev librocksdb-dev libprotobuf-dev protobuf-compiler libpthread-stubs0-dev zlib1g-dev libevent-dev
   ```

2. Build the source code:

   ```
   mkdir build && cd build/ && cmake .. && make
   ```

## Tests

### Test 2.0

This test checks simple put and get requests into the key value store through the controller and router.

To execute this test independently, run:
```
python3 tests/test_simple_kvs.py
```

### Test 2.1

This test checks if sharding has been implemented correctly. The test assumes that no new shards are added to the cluster once puts and gets have been performed.

To execute this test independently, run:
```
python3 tests/test_static_sharding.py
```

### Test 2.2

This test checks to see if a new shard joining the cluster has been handled properly i.e. shards can join after puts and gets have been performed.
Keys and their values would have to be redistributed to ensure that hashed mappings stay valid.

To execute this test independently, run:
```
python3 tests/test_dynaic_sharding.py
```

## References

* [Protobufs](https://developers.google.com/protocol-buffers/docs/cpptutorial)
* [RocksDB](http://rocksdb.org/docs/getting-started.html)
* [Chord](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)
* [C++ implementation of chord](https://github.com/sit/dht)

