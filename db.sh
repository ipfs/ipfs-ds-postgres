#!/bin/bash
docker stop ipfs-ds-postgres-db
docker rm ipfs-ds-postgres-db
docker run  --name ipfs-ds-postgres-db -p 5432:5432 -it -e POSTGRES_PASSWORD=postgres postgres:11 # -c log_statement=all
