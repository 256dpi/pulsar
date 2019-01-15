#!/usr/bin/env bash

# set java home
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

# remove namespace
pulsar-admin namespaces delete public/test

# create namespace
pulsar-admin namespaces create --clusters standalone public/test
