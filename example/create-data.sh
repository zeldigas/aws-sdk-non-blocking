#!/usr/bin/env bash

aws dynamodb put-item \
    --table-name $1 \
    --item '{
        "Artist": {"S": "Acme Band"},
        "SongTitle": {"S": "Happy Day"} ,
        "Album": {"S": "Songs About Life"} }'