#! /bin/bash

echo 'Waiting for S3 to initialize...'
until curl -s s3-storage:8000 > /dev/null
do
    sleep 5
    echo 'Waiting for S3 to initialize...'
done
echo Connected to S3
