Apache Kafka Wire Protocol
==========================

Protocol Primitive Types
------------------------

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProtocolPrimitiveTypes

Request/Response Prefix
-----------------------

Each request and response is prefixed by the size of the request.

Request Header
--------------

API KEY        int16
API VERSION    int16
CORRELATION ID int32
CLIENT ID      string

 0             1               2               3               4
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|            API KEY            |          API VERSION          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        CORRELATION ID                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       CLIENT ID LENGTH        |           CLIENT ID           /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               /
/                       CLIENT ID (cont'd)                      /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Produce Request
---------------

REQUIRED ACKS    int16
TIMEOUT          int32
NUM TOPICS       int32
TOPIC            string
NUM PARTITIONS   int32
PARTITION        int32
MESSAGE SET SIZE int32
MESSAGE SET      A Message Set is a sequence of Messages.

 0             1               2               3               4
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         REQUIRED ACKS         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            TIMEOUT                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           NUM TOPICS                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         TOPIC LENGTH          |             TOPIC             /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               /
/                        TOPIC (cont'd)                         /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        NUM PARTITIONS                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           PARTITION                           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       MESSAGE SET SIZE                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/                                                               /
/                          MESSAGE SET                          /
/                                                               /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Message
-------

OFFSET       int64
MESSAGE SIZE int32
CRC          int32
MAGIC        int8
ATTRIBUTES   int8
KEY          byte string
VALUE        byte string

Keys are optional. A key length of -1 indicates a NULL key.

 0             1               2               3               4
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                       OFFSET (8 bytes)                        |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         MESSAGE SIZE                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                              CRC                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     MAGIC     |  ATTRIBUTES   |          KEY LENGTH           /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/      KEY LENGTH (cont'd)      |              KEY              /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               /
/                         KEY (cont'd)                          /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         VALUE LENGTH                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/                                                               /
/                             VALUE                             /
/                                                               /
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Produce Request Example
-----------------------

SIZE: 78 (0x00 0x00 0x00 0x4E)

REQUEST HEADER
--------------

    api key:        0     (0x00 0x00)
    api version:    0     (0x00 0x00)
    correlation id: 1     (0x00 0x00 0x00 0x01)
    client id:      "foo" (0x00 0x03 0x66 0x6F 0x6F)

PRODUCE REQUEST
---------------

    required acks:    1      (0x00 0x01)
    timeout:          1500   (0x00 0x00 0x05 0xDC)
    num topics:       1      (0x00 0x00 0x00 0x01)
    topic:            "test" (0x00 0x04 0x74 0x65 0x73 0x74)
    num partitions:   1      (0x00 0x00 0x00 0x01)
    partition:        0      (0x00 0x00 0x00 0x00)
    message set size: 37     (0x00 0x00 0x00 0x25)

MESSAGE SET
-----------

    Message #1:

        offset:     0  (0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00)
        size:       25 (0x00 0x00 0x00 0x19)
        crc:        0x73 0xAC 0xF7 0x7C
        magic:      0 (0x00)
        attributes: 0 (0x00)
        key:        NULL (0xFF 0xFF 0xFF 0xFF)
        value:      "hello world" (0x00 0x00 0x00 0x0B
                                   0x68 0x65 0x6C 0x6C 0x6F 0x20
                                   0x77 0x6F 0x72 0x6C 0x64)

Hex Dump
--------

0x00 0x00 0x00 0x4E 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01
0x00 0x03 0x66 0x6F 0x6F 0x00 0x01 0x00 0x00 0x05 0xDC 0x00
0x00 0x00 0x01 0x00 0x04 0x74 0x65 0x73 0x74 0x00 0x00 0x00
0x01 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x25 0x00 0x00 0x00
0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x19 0x73 0xAC 0xF7
0x7C 0x00 0x00 0xFF 0xFF 0xFF 0xFF 0x00 0x00 0x00 0x0B 0x68
0x65 0x6C 0x6C 0x6F 0x20 0x77 0x6F 0x72 0x6C 0x64