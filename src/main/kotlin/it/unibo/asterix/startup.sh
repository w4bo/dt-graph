#!/bin/bash


install_redis() {
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
    redis-cli -h "$REDIS_HOST" INCR "$BARRIER_NAME"
}

# Function to check if all clients have reached the barrier
check_barrier() {
    local current_count=$(redis-cli -h "$REDIS_HOST" GET "$BARRIER_NAME")
    echo "Current count: $current_count"
    if [[ "$current_count" -ge "$DATASOURCES_NUMBER" ]]; then
        return 0  # All clients have reached the barrier
    else
        return 1  # Not all clients have reached the barrier yet
    fi
}

# Function to attempt a curl request
retry_curl() {
    #sleep $((RANDOM % 100))  # Avoid all data sources to connect at the same time

    echo "Trying to open a Data Feed to ASTERIX_PORT=$ASTERIX_PORT (Attempt $((attempt+1))/$max_attempts)"

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

    curl -X POST "http://asterixdb:19002/query/service" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "statement=$SQL_STATEMENT" \
      --data-urlencode "pretty=true" \
      --data-urlencode "mode=immediate"

    return $?
}


install_redis

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

        # Retry the POST request until it succeeds
        retry_curl
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
increment_barrier

# Wait until all clients reach the barrier
while ! check_barrier; do
    echo "Waiting for all replicas to complete the POST request..."
    sleep 1
done

echo "All clients have reached the barrier, proceeding to the next phase..."
        
# Sleep before executing Java
sleep 5

# Start Java process
java -jar /asterix/STS-Graph-all.jar
