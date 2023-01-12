#!/bin/bash

spark-daemon.sh stop org.apache.spark.deploy.worker.Worker 2 --webui-port 8080 --port 65510 --cores 2 --memory 4g spark://192.168.0.1:7077