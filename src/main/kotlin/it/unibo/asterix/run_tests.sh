#!/bin/bash

# Define the arrays for "replicas" and "cluster_machines"
n=(8 16 32 64 128)
cluster_machines=(1 2 4)
SERVICE_NAME="asterixStack_datasources"
TEST_ID=0  # Initialize the TEST_ID

# Function to check the status of the service
check_service_status() {
    # Get the current state of the service
    state=$(docker service ps $SERVICE_NAME --format "{{.CurrentState}}")
    echo "Service state: $SERVICE_NAME: $state"

    # If the service state contains 'Shutdown', return 0 (indicating shutdown)
    if [[ "$state" == *"Shutdown"* ]]; then
        return 0  # Service in shutdown state
    fi

    # Otherwise, return 1 (service is not in shutdown state)
    return 1  # Service not in shutdown state
}

# Function to execute the workload for a specific compose file and replicas value
run_workload() {
  local compose_file=$1
  local replicas=$2
  local machines=$3

  echo "Modifying replicas value to: $replicas and DATASOURCES_NUMBER to $replicas"

  # Increment the TEST_ID
  TEST_ID=$((TEST_ID + 1))

  # Dynamically replace the "replicas" and "DATASOURCES_NUMBER" values in the YAML file
  sed -i "s/replicas: [0-9]*/replicas: $replicas/" "$compose_file"
  sed -i "s/DATASOURCES_NUMBER: [0-9]*/DATASOURCES_NUMBER: $replicas/" "$compose_file"
  sed -i "s/TEST_ID: [0-9]*/TEST_ID: $TEST_ID/" "$compose_file"
  sed -i "s/ASTERIX_MACHINES_COUNT: [0-9]*/ASTERIX_MACHINES_COUNT: machines/" "$compose_file"

  # Deploy the stack with the modified YAML file
  echo "Running docker stack deploy with replicas=$replicas and TEST_ID=$TEST_ID"
  docker stack deploy -c "$compose_file" asterixStack

  echo "Waiting for test to complete..."

  # Loop to wait for the service to finish
  while true; do
      # Check if the service is in shutdown state
      if check_service_status; then
          echo "The service $SERVICE_NAME has terminated, proceeding to the next test..."

          # Remove the stack after the service finishes
          docker stack rm asterixStack
          break  # Exit the loop after completing the test
      else
          echo "The service $SERVICE_NAME is not yet terminated. Waiting..."
          sleep 10  # Pause for 10 seconds before checking again
      fi
  done

  # Wait until the log command finishes, then proceed to the next value
  echo "Logs completed for replicas=$replicas. Moving on to the next value."

  # Optional: Insert a pause between iterations if needed
  # sleep 5
}

# Iterate over the values of "cluster_machines"
for c in "${cluster_machines[@]}"; do
  echo "Starting tests for cluster size: $c machines"

  # Determine the appropriate Docker Compose file based on the value of c
  if [ "$c" -eq 1 ]; then
    compose_file="data-source-single-machine-compose.yaml"
  elif [ "$c" -eq 2 ]; then
    compose_file="data-source-two-machine-compose.yaml"
  elif [ "$c" -eq 4 ]; then
    compose_file="data-source-four-machine-compose.yaml"
  else
    echo "Invalid cluster size: $c. Skipping..."
    continue
  fi

  # Iterate over the values in the "n" array (for different replicas)
  for replicas in "${n[@]}"; do
    # Run the workload for the selected compose file and replica value
    run_workload "$compose_file" "$replicas" "$c"
  done

  echo "Completed all tests for cluster size: $c machines"
done

echo "The entire process has been completed for all cluster sizes and replica values!"
