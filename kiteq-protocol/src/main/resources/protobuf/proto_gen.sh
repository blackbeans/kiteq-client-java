#!/bin/bash
protoc -I=./schema/ --java_out=./generated/ ./schema/kite_remoting.proto