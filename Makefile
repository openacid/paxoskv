gen:
	# go get github.com/golang/protobuf/{proto,protoc-gen-go}@v1.2.0
	protoc --proto_path=proto --go_out=plugins=grpc:paxoskv paxoskv.proto
