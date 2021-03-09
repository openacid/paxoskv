#!/bin/sh


fns=$(find . -name "*.go" \
    | grep -v "\.pb\.go" \
    | grep -v "_test.go")

echo fns: $fns

fns=./paxoskv/impl.go

cat $fns \
    | grep -v "^	*//" \
    | grep -v "^$" \
    | grep -v "pretty\." \
    | wc
