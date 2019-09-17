# CHORD Distributed Hash Table

Python implementation of [CHORD Distributed Hash Table](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)). A novel p2p file distribution/storage functionality is also implemented on top of the DHT.

### Usage
##### Root
`./dht_peer -m 1 <-p own_port> <-h own_hostname>`    

Root node must be initialized first.

##### Peers

`./dht_peer <-p own_port> <-h own_hostname> <-r root_port> <-R root_hostname>`

##### Client

`./dht_client <-p own_port> <-h own_hostname> <-r root_port> <-R root_hostname>`

Clients are able to store files and retrieve files from the network, both iteratively and recursively.
