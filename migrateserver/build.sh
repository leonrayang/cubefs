#!/bin/bash

Version=$(git describe --abbrev=0 --tags 2>/dev/null)
BranchName=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
CommitID=$(git rev-parse HEAD 2>/dev/null)
BuildTime=$(date +%Y-%m-%d\ %H:%M)
LDFlags="-X github.com/chubaofs/chubaofs/proto.Version=${Version} \
    -X github.com/chubaofs/chubaofs/proto.CommitID=${CommitID} \
    -X github.com/chubaofs/chubaofs/proto.BranchName=${BranchName} \
    -X 'github.com/chubaofs/chubaofs/proto.BuildTime=${BuildTime}'"
MODFLAGS=""

go build $MODFLAGS -ldflags "${LDFlags}" -o migrate-server