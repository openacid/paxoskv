
gen: gen-go

gen-go:
	protoc  --proto_path=proto \
		--go_out=plugins=grpc:paxoskv \
		--go_opt=paths=source_relative \
		paxoskv.proto
