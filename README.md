# DistributedRDFEngine
#### Offline process RDF Triple on Hadoop
* build id-mapping 
* build bitset signature of each triples
* Repeat Kmeans over signatures forming VS-tree
* Store VS-tree in HBase
#### Online query SPARQL with MPI (Thrift to HBase)
* Encode SPARQL into signatures
* Slaves: Filter signature in VS-tree with query signature 
* Master: Join partial matches in parallel
* Report answers
