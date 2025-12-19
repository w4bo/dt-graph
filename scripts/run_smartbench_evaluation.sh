#!/bin/bash


IFS=',' read -r -a THREADS <<< "$THREAD"
IFS=',' read -r -a DATASET_SIZES <<< "$DATASET_SIZE"
IFS=',' read -r -a QUERY_SELECTIVITIES <<< "$QUERY_SELECTIVITY"

mkdir -p dt_graph/logs

# Test ingestion performances
for DATASET in "${DATASET_SIZES[@]}"

do
./scripts/download_dataset.sh $DATASET

  export DATASET_SIZE="$DATASET"

  for SELECTIVITY in "${QUERY_SELECTIVITIES[@]}"
  do
    for EVALTHREAD in "${THREADS[@]}"
    do
      export THREAD=$EVALTHREAD
      echo "Running SmartBench ingestion performance evaluation with $DATASET dataset size and $THREAD threads"
      ./gradlew test --tests TestIngestion --rerun-tasks 2>&1 | tee "dt_graph/logs/ingestion_size${DATASET}_THREADS${EVALTHREAD}.log"
      echo "Running SmartBench query performance evaluation with $DATASET dataset size and $THREAD threads"
      ./gradlew test --tests TestQuerySmartBench --rerun-tasks 2>&1 | tee -a "dt_graph/logs/query_size${DATASET}_THREADS${EVALTHREAD}.log"
    done
  done
done

curl -X POST http://asterixdb:19002/query/service \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "statement=SELECT VALUE dv FROM Metadata.\`Dataverse\` dv WHERE dv.DataverseName = \"Measurements_Dataverse\";"

curl -X POST http://asterixdb:19002/query/service \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "statement=USE Measurements_Dataverse%3B select count(*) from dataset_1"