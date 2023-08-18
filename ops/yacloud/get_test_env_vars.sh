#!/usr/bin/env bash

kubectl get pods -n integration-tests -ojson "$1" \
| jq '.spec.containers[0].env' \
| jq 'map( "\(.name)=\(.value)" ) | join(";")' \
| tr -d \" \
