#!/bin/sh

fns=$(find . -name "*.go" |
  grep -v "\.pb\.go" |
  grep -v "_test.go")

echo fns: "$fns"

# fns=./paxoskv/impl.go

for fn in "$fns"; do
  echo "$fn"
  cat "$fn" |
    grep -v "^	*//" |
    grep -v "^$" |
    grep -v "pretty\." |
    grep -v "dd(" |
    wc
done

cat "$fns" |
  grep -v "^	*//" |
  grep -v "^$" |
  grep -v "pretty\." |
  grep -v "dd(" |
  wc
