#!/usr/bin/env bash

# download proto file
wget -q -O pulsar.proto https://raw.githubusercontent.com/apache/pulsar/v2.2.1/pulsar-common/src/main/proto/PulsarApi.proto

# format proto file
clang-format -style="{BasedOnStyle: llvm, ColumnLimit: 120}" -i pulsar.proto

# prepend package name
sed -i '' -e 's/message Schema {/option go_package = \"pp\";\
message Schema {/' pulsar.proto

# generate go file
protoc --gofast_out=. pulsar.proto
