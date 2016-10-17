# README #

Distributed triple-store big3store is based on dataflow architecture
of query processing. Each query is a tree of algebra operations that
is dynamically mapped to the tree composed of processes interconnected
by streams of graphs, i.e., sets of triples. The scheduler that maps
query trees to set of processes balances the computation load among
the servers of cluster.

Triple-store of big3store is distributed into partitions stored in 
columns and replicated into rows in the array of data servers. Data
distribution is achieved by means of semantic distribution function
that splits the triples on the basis of the
relation of each particular triple to the taxonomy of RDF classes
and properties. 

* Installation and deployment
* big3store e-documentation
* Contribution guidelines
* Contact and discussion

## Installation and deployment ##

The basic tasks of the big3store installation and deployment process 
are described.

* [Installation](src/HOWTO/install.md)
* [Configure system](src/HOWTO/configure-system.md)
* [Loading triple-store](src/HOWTO/load-triples.md)
* [big3store user interface](src/HOWTO/user-interface.md)

## big3store documentation ##

The documentation of big3store system is provided in edoc format. The types, 
data structures and functions are described for the Erlang modules of big3store.

[big3store Edoc documentation](doc/index.html)

## Contribution guidelines ##

In the case that you would like to contribute to the development of big3store please 
send a mail to Kiyoshi Nitta (knitta@yahoo-corp.jp) or Iztok Savnik (iztok.savnik@famnit.upr.si).

## Contact and discussion ##

[big3store mailing list](https://groups.google.com/forum/#!forum/big3store)
