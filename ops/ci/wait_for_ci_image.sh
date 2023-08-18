#!/usr/bin/env sh

set -eux

# initial wait for build, sh due to missing bash in the standard docker image

c=0

while [ "$c" -lt 100 ]
do
  c=`expr $c + 1`
  if docker pull $REF; then
    c=1000
  else
    echo "sleep"
    sleep 10
  fi
done

docker inspect $REF
