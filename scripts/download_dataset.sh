#!/bin/bash

DATASET_SIZE=$1
# MEGA link (not OneDrive!)
LINK="https://big.csr.unibo.it/downloads/stgraph/stgraph/"
OUTPUT_DIR="${2:-"/dt_graph/src/main/resources/dataset/smartbench"}"

mkdir -p "$OUTPUT_DIR"

echo "Downloading dataset ..."
cd "$OUTPUT_DIR" || exit 1

curl -L -o "./${DATASET_SIZE}.tar" "${LINK}${DATASET_SIZE}.tar"

if [[ -f "${DATASET_SIZE}.tar" ]]; then
    echo "Downloaded: ${DATASET_SIZE}.tar"
    tar -xvf "${DATASET_SIZE}.tar"
    rm "${DATASET_SIZE}.tar"
else
    echo "Error: Something went wrong while downloading dataset ${DATASET_SIZE}!"
    exit 1
fi