# AsterixDB basic evaluation performances

## Requirements

- Docker Swarm Cluster e.g.:
  - CC: IP1
  - NC1: IP2
  - NC2: IP3
- Accessible NFS folder:
  - NFS: NFS_IP
- AsterixDataSource JAR

## Initial Setup

- In {NFS_FOLDER}, create following folders:
  - {NFS_FOLDER}/asterixdb/jar
  - {NFS_FOLDER}/asterixdb/logs_dir
  - {NFS_FOLDER}/asterixdb/results

- Inside {NFS_FOLDER}/asterixdb/jar place the following files (package it.unibo.evaluation.asterixdb.scripts):
  - download_asterix.sh
  - setup.txt
  - startup.sh
  - STS_Graph-all.jar

- Modify startup.sh and update AsterixDB NC IPs in line 60

## Deployment

1. Start AsterixDB cluster
2. SSH into one cluster machine
3. Create folder ./asterix_eval and cd into it.
4. Copy the following files
    - data-source-two-machine-compose.yaml (package it.unibo.deploy.asterixdb)
    - run_tests.sh (package it.unibo.evaluation.asterixdb.scripts)
5. Update test parameters within run_tests.sh in lines [4-7].
6. Run ./run_tests.sh
7. Results will then be available in
