#!/bin/bash

export PYSPARK_PYTHON=$(which python3.12)
export PYSPARK_DRIVER_PYTHON=$(which python3.12)

echo "PYSPARK_PYTHON is set to: $PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON is set to: $PYSPARK_DRIVER_PYTHON"