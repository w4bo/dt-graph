#!/bin/bash

SERVICE_NAME="datasources"

set -e

TS_ID=$(( RANDOM % 5001 ))
ASTERIX_PORT=$((10000 + TS_ID))

echo "ASTERIX_PORT=$ASTERIX_PORT"

SQL_STATEMENT="USE Measurements_Dataverse;
DROP FEED MeasurementsFeed_${TS_ID} IF EXISTS;
CREATE FEED MeasurementsFeed_${TS_ID} WITH {
  \"adapter-name\": \"socket_adapter\",
  \"sockets\": \"127.0.0.1:${ASTERIX_PORT}\",
  \"address-type\": \"IP\",
  \"type-name\": \"Measurement\",
  \"policy\": \"Spill\",
  \"format\": \"adm\"
};
CONNECT FEED MeasurementsFeed_${TS_ID} TO DATASET OpenMeasurements;
START FEED MeasurementsFeed_${TS_ID};"

# Funzione per tentare la richiesta con retry
retry_curl() {
  local success=1
  while [ $success -ne 0 ]; do
    echo "Trying to open a Data Feed to ASTERIX_PORT=$ASTERIX_PORT"

    curl -X POST "http://asterixdb:19002/query/service" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "statement=$SQL_STATEMENT" \
      --data-urlencode "pretty=true" \
      --data-urlencode "mode=immediate"

    success=$?

    if [ $success -ne 0 ]; then
      echo "Curl fallito. Genero una nuova porta..."
      TS_ID=$(( RANDOM % 5001 ))
      ASTERIX_PORT=$((10000 + TS_ID))
      sleep 2
    fi
  done
  echo "Curl eseguito con successo su ASTERIX_PORT=$ASTERIX_PORT"
  export TS_ID=$TS_ID
}

check_service_status() {
    state=$(docker service ps $SERVICE_NAME --format "{{.CurrentState}}")
    echo "Stato attuale del servizio $SERVICE_NAME: $state"
    if [[ "$state" == *"Shutdown"* ]]; then
        return 0  # Servizio in stato di shutdown
    fi
    return 1  # Servizio non in stato di shutdown
}
# Aspetta un po' prima di iniziare la richiesta
sleep 20

# Esegui il retry finché curl non ha successo
retry_curl

# Avvia Java
java -jar /asterix/STS-Graph-all.jar

