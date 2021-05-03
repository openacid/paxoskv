#!/bin/sh


# go get github.com/gogo/protobuf/protoc-gen-gogofast
# go get github.com/gogo/protobuf/protoc-gen-gogofaster
# go get github.com/gogo/protobuf/protoc-gen-gogoslick

protoc -I=. \
    -I="$GOPATH"/src \
    -I="$GOPATH"/src/github.com/gogo/protobuf/protobuf \
    --proto_path=proto \
    --gogofaster_out=plugins=grpc:paxoskv \
    paxoskv.proto
