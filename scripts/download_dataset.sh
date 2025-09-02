#!/bin/bash
set -exo

DATASET_SIZE=$1
LINK="https://big.csr.unibo.it/downloads/stgraph/stgraph/"
INTERNAL_LINK="137.204.74.24/downloads/stgraph/stgraph/"
OUTPUT_DIR="${2:-"/dt_graph/src/main/resources/dataset/smartbench"}"
FILENAME="${DATASET_SIZE}.tar"

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR" || exit 1

echo "Downloading dataset ..."
# curl -L -o "./${DATASET_SIZE}.tar" "${LINK}${DATASET_SIZE}.tar"
if ! wget --no-check-certificate --tries=3 "${LINK}${FILENAME}"; then
    echo "Primary link failed, trying backup..."
    wget --no-check-certificate --tries=3 "${INTERNAL_LINK}${FILENAME}" || {
        echo "Error: failed to download from backup too!"
        exit 1
    }
fi

if [[ -f "${DATASET_SIZE}.tar" ]]; then
    echo "Downloaded: ${DATASET_SIZE}.tar"
    tar -xvf "${DATASET_SIZE}.tar"
    rm "${DATASET_SIZE}.tar"
else
    echo "Error: Something went wrong while downloading dataset ${DATASET_SIZE}!"
    exit 1
fi