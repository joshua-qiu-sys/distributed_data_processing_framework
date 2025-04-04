#!/bin/bash

confluent kafka topic create uncatg_landing_zone \
    --url http://localhost:8088 \
    --partitions 3 \
    --replication-factor 3 \
    --no-authentication \
    --config min.insync.replicas=2