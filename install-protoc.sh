#!/bin/sh

PROTOBUF_VERSION=3.10.0
PROTOC_FILENAME=protoc-${PROTOBUF_VERSION}-linux-x86_64.zip

(

cd /home/travis

wget https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/$PROTOC_FILENAME
unzip $PROTOC_FILENAME
bin/protoc --version

)
