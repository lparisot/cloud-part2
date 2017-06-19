## Cloud computing part 2

Implements MP2Node, the key-value store.

## Functionalities
* a key-value store supporting CRUD operations
* load-balancing (via a consistent hashing ring to hash both servers and keys)
* fault-tolerance up to 2 failures (by replicating each key 3 times to 3 successive nodes in the ring)
* quorum consistency level for both reads and writes (at least 2 replicas)
* stabilization after failure (recreate 3 replicas)

See MP2-specification-document.pdf file.
