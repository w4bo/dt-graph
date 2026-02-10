#!/bin/bash


install_redis() {
  echo "Installing REDIS"
  apt-get update && apt-get install -y redis-tools build-essential curl > /dev/null 2>&1
}
# Function to acquire the lock
get_lock() {
    redis-cli -h "$REDIS_HOST" SET "$LOCK_NAME" "locked" EX 10 NX
}

# Function to release the lock
release_lock() {
    redis-cli -h "$REDIS_HOST" DEL "$LOCK_NAME"
}

# Function to increment the barrier counter
increment_barrier() {
    local BARRIER="$1"
    redis-cli -h "$REDIS_HOST" INCR "$BARRIER"
}

# Function to check if all clients have reached the barrier
check_barrier() {
    local current_count=$(redis-cli -h "$REDIS_HOST" GET "$BARRIER_READY_NAME")
    echo "Current count: $current_count"
    if [[ "$current_count" -ge "$DATASOURCES_NUMBER" ]]; then
        return 0  # All clients have reached the barrier
    else
        return 1  # Not all clients have reached the barrier yet
    fi
}

create_dataset() {
  DATASET="dataset_TS$TS_ID"
  export DATASET
  echo "Opening dataset $DATASET"
  SQL_STATEMENT="
    USE $DATAVERSE;
    CREATE DATASET $DATASET(Measurement)IF NOT EXISTS primary key timestamp;
    CREATE INDEX measurement_location_$TS_ID on $DATASET(location) type rtree;
  "
  curl -X POST "http://$ASTERIX_IP:19002/query/service" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "statement=$SQL_STATEMENT" \
    --data-urlencode "pretty=true" \
    --data-urlencode "mode=immediate"

  return $?
}

# Function to attempt a curl request
open_datafeed() {
    #sleep $((RANDOM % 100))  # Avoid all data sources to connect at the same time

    echo "Trying to open a Data Feed to ASTERIX_PORT=$ASTERIX_PORT (Attempt $((attempt+1))/$max_attempts)"

    # AsterixDB Node controllers IPs
    IP_OPTIONS=("192.168.30.104" "192.168.30.105" "192.168.30.107" "192.168.30.108")

    DATAFEED_IP=${IP_OPTIONS[$((RANDOM % 2))]}

    export DATAFEED_IP

    SQL_STATEMENT="USE $DATAVERSE;
    DROP FEED MeasurementsFeed_${TS_ID} IF EXISTS;
    CREATE FEED MeasurementsFeed_${TS_ID} WITH {
      \"adapter-name\": \"socket_adapter\",
      \"sockets\": \"${DATAFEED_IP}:${ASTERIX_PORT}\",
      \"address-type\": \"IP\",
      \"type-name\": \"Measurement\",
      \"policy\": \"Spill\",
      \"format\": \"adm\"
    };
    CONNECT FEED MeasurementsFeed_${TS_ID} TO DATASET $DATASET;
    START FEED MeasurementsFeed_${TS_ID};"

    echo $SQL_STATEMENT
    curl -X POST "http://$ASTERIX_IP:19002/query/service" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "statement=$SQL_STATEMENT" \
      --data-urlencode "pretty=true" \
      --data-urlencode "mode=immediate"

    return $?
}

logs_dir="/logs_dir/${dataSourcesNumber}dataSources_maxIterations${maxIteration}_${asterixClusterMachines}cluster"
mkdir -p $logs_dir


install_redis

sleep 10
# Initialize variables
TS_ID=$(( RANDOM % 5001 ))
ASTERIX_PORT=$((10000 + TS_ID))
success=1
attempt=0
max_attempts=10

echo "ASTERIX_PORT=$ASTERIX_PORT"

while [ $success -ne 0 ] && [ $attempt -lt $max_attempts ]; do
    LOCK=$(get_lock)

    if [[ "$LOCK" == "OK" ]]; then
        echo "Lock acquired, starting the POST request..."

        create_dataset
        # Retry the POST request until it succeeds
        open_datafeed
        success=$?

        if [ $success -ne 0 ]; then
            echo "Curl failed on ASTERIX_PORT=$ASTERIX_PORT. Retrying..."
            TS_ID=$(( RANDOM % 5001 ))
            ASTERIX_PORT=$((10000 + TS_ID))
            attempt=$((attempt + 1))
            sleep 2
        else
            echo "Curl executed successfully on ASTERIX_PORT=$ASTERIX_PORT"
            export TS_ID=$TS_ID
            export ASTERIX_PORT=$ASTERIX_PORT
            break  # Exit retry loop
        fi

    else
        echo "Another replica is already executing the POST request, waiting..."
        sleep 2
    fi
done

if [ $success -ne 0 ]; then
    echo "Curl failed after $max_attempts attempts. Exiting."
    exit 1
fi

release_lock  # Release the lock

# Increment the barrier counter only after a successful POST request
increment_barrier $BARRIER_READY_NAME

# Wait until all clients reach the barrier
while ! check_barrier; do
    echo "Waiting for all replicas to complete the POST request..."
    sleep 1
done

echo "All clients have reached the barrier, proceeding to the next phase..."

# Sleep before executing Java
sleep 5

# Start Java process
java -jar /asterix/STS-Graph-all.jar #> "${logs_dir}/tsId${TS_ID}_$(date +%Y%m%d_%H%M%S).log" 2>&1 &

# Trova la cartella dentro /asterix_statistics
src_dir=$(find /asterix_statistics -mindepth 1 -maxdepth 1 -type d)

# Estrai il nome della cartella
folder_name=$(basename "$src_dir")

# Crea la cartella di destinazione se non esiste
mkdir -p "/asterix_statistics_nfs/$folder_name"

# Copia il contenuto (assumendo ci sia un solo file dentro)
cp "$src_dir"/* "/asterix_statistics_nfs/$folder_name/"


increment_barrier $BARRIER_COMPLETED_NAME
