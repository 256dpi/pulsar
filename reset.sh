#!/usr/bin/env bash

# enable echo
set -x

# set java home
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

# remove all topics
pulsar-admin topics list public/test | xargs -t -L1 pulsar-admin topics delete -f

# remove namespace
pulsar-admin namespaces delete public/test

# create namespace
pulsar-admin namespaces create -c standalone public/test
