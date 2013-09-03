libkafka
========

C Library for interacting with Apache Kafka 0.8. I started this library because
there doesn't seem to be a decent Kafka library for C.

License: 2-Clause BSD

You need the Apache ZooKeeper C library installed (zookeeper_mt).

For setting up a dev cluster on a single box, I used this as an example: [Running a Multi-Broker Apache Kafka 0.8 Cluster on a Single Node](http://www.michael-noll.com/blog/2013/03/13/running-a-multi-broker-apache-kafka-cluster-on-a-single-node/)

I'm also documenting the wire protocol as I go along. Documentation can be found
under the docs/ directory in this repo.

*Warning!* This isn't ready for primetime yet unless you're feeling adventurous.