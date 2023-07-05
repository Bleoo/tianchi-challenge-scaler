#!/bin/bash

pkill -9 serverless-sim
trap 'kill ${SIM_PID}' EXIT

#set -e # Enable exit on error

command -v curl >/dev/null 2>&1 || { echo >&2 "Curl is required but it's not installed. Exiting."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo >&2 "Jq is required but it's not installed. Exiting."; exit 1; }

# Start the serverless-sim in the background
./serverless-sim &

# Get the process ID of serverless-sim for later use
SIM_PID=$!

# Check if the scaler is ready to accept connections
COUNTER=0
while true; do
  nc -z localhost 9001 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Scaler is ready to accept connections."
    break
  else
    let COUNTER=COUNTER+1
    if [ $COUNTER -gt 20 ]; then
      echo "Scaler is not ready after 20 minutes. Exiting."
      exit 1
    fi
    sleep 60
  fi
done

# Check if the simulator is ready to accept connections
COUNTER=0
while true; do
  nc -z localhost 9000 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Simulator is ready to accept connections."
    break
  else
        let COUNTER=COUNTER+1
        if [ $COUNTER -gt 20 ]; then
          echo "Simulator is not ready after 200s. Exiting."
          exit 1
        fi
    sleep 10
  fi
done


# Define data set and result directory
DATA_DIR=/tmp/data

RESULT_DIR=/tmp/result

# Create result directory if not exist
if [ ! -d "${RESULT_DIR}" ]; then
    mkdir -p "${RESULT_DIR}" || {
        echo "Cannot create ${RESULT_DIR}, maybe due to insufficient permissions."
        exit 1
    }
fi


# If user specifies datasets, use them. Otherwise, use all datasets in ${DATA_DIR}
if [ "$#" -gt 0 ]; then
    echo "User specified datasets: $@"
    datasets=("$@")
else
    echo "No dataset specified. Using all datasets in ${DATA_DIR}"
    datasets=($(ls -d ${DATA_DIR}/* | xargs -n 1 basename))
fi

# Define a function to check if the simulation has ended
function check_simulation_end {
    local response=$(curl -s http://127.0.0.1:9000/)
    local end_time=$(echo $response | jq -r '.endTime')
    if [[ $end_time == "not end yet" ]]; then
        return 1
    else
        return 0
    fi
}

for dataset in "${datasets[@]}"; do
    folder=${DATA_DIR}/$dataset
    if [ -d "$folder" ]; then
        folder_name=$(basename $folder)
        echo "Starting simulation: ${folder_name}"
        curl -v -X POST http://127.0.0.1:9000/start \
             -d "{\"id\": \"$folder_name\", \"requestFilePath\": \"$folder/requests\", \"metaFilePath\": \"$folder/metas\"}" \
             -H 'Content-Type: application/json'

        while ! check_simulation_end; do
            sleep 10
        done

        sleep 5

        echo "Simulation finished: $folder_name"

        curl -o /tmp/result/result_${folder_name}.txt http://127.0.0.1:9000/ || {
                    echo "Failed to save simulation results for ${folder_name}."
                    exit 1
                }
    else
        echo "Dataset ${folder_name} does not exist. Please check the dataset name and try again."
        exit 1
    fi
done

