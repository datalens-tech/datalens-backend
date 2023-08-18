#!/usr/bin/env bash

docker network create --ipv6 --driver bridge --subnet=2001:fdea::1:0/112 --gateway=2001:fdea::1:1 bi-uploads
