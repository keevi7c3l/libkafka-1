[![Build Status](https://travis-ci.org/davidreynolds/libkafka.png?branch=master)](https://travis-ci.org/davidreynolds/libkafka)

C library for interacting with Apache Kafka 0.8.

License: 2-Clause BSD

You need the Apache ZooKeeper C library installed (zookeeper_mt). Zookeeper and
jansson (for JSON parsing) can probably be removed from the producer since the
Metadata API provides everything a producer needs.

For setting up a dev cluster on a single box, I used this as an example: [Running a Multi-Broker Apache Kafka 0.8 Cluster on a Single Node](http://www.michael-noll.com/blog/2013/03/13/running-a-multi-broker-apache-kafka-cluster-on-a-single-node/)

I'm also documenting the wire protocol as I go along. Documentation can be found
under the docs/ directory in this repo or on http://tapestry.io/blog/kafka_wire_format.html

*Warning!* This isn't ready for production yet.

At the moment I only support producer and metadata requests/responses. I provide
APIs for producing a single message to a given topic and batching multiple
messages to different topics and partitions in a single go.
