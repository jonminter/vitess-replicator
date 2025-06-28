#!/bin/bash

CWD=$(pwd)

VITESS_PROTOS="https://github.com/vitessio/vitess"
mkdir -p /tmp/vitess
cd /tmp/vitess
git clone --depth=1 $VITESS_PROTOS
if [ $? -ne 0 ]; then
    echo "Failed to clone vitess repository"
    exit 1
fi

cd ${CWD}
mkdir -p ./proto/vitess
cp /tmp/vitess/vitess/proto/* ./proto/vitess/
echo "vtgate.proto downloaded successfully"