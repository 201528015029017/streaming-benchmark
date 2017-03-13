#!/bin/bash
# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
set -o pipefail
set -o errtrace
set -o nounset
set -o errexit

LEIN=${LEIN:-lein}
MVN=${MVN:-mvn}
GIT=${GIT:-git}
MAKE=${MAKE:-make}

KAFKA_VERSION=${KAFKA_VERSION:-"0.10.0.1"}
REDIS_VERSION=${REDIS_VERSION:-"3.0.7"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.10"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"4"}
FLINK_VERSION=${FLINK_VERSION:-"1.2.0"}

REDIS_DIR="/home/yelei/redis-3.0.7/"

KAFKA_HOST="9.96.191.32"
KAFKA_PORT="21005"
ZK_HOST="9.96.191.32"
ZK_PORT="24002"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
TOPIC=${TOPIC:-"ad-events"}
PARTITIONS=${PARTITIONS:-1}
LOAD=${LOAD:-1000}
CONF_FILE=./conf/localConf.yaml
TEST_TIME=${TEST_TIME:-240}

pid_match() {
   local VAL=`ps -aef | grep "$1" | grep -v grep | awk '{print $2}'`
   echo $VAL
}

start_if_needed() {
  local match="$1"
  shift
  local name="$1"
  shift
  local sleep_time="$1"
  shift
  local PID=`pid_match "$match"`

  if [[ "$PID" -ne "" ]];
  then
    echo "$name is already running..."
  else
    "$@" &
    sleep $sleep_time
  fi
}

stop_if_needed() {
  local match="$1"
  local name="$2"
  local PID=`pid_match "$match"`
  if [[ "$PID" -ne "" ]];
  then
    kill "$PID"
    sleep 1
    local CHECK_AGAIN=`pid_match "$match"`
    if [[ "$CHECK_AGAIN" -ne "" ]];
    then
      kill -9 "$CHECK_AGAIN"
    fi
  else
    echo "No $name instance found to stop"
  fi
}

fetch_untar_file() {
  local FILE="download-cache/$1"
  local URL=$2
  if [[ -e "$FILE" ]];
  then
    echo "Using cached File $FILE"
  else
	mkdir -p download-cache/
    wget -O "$FILE" "$URL"
  fi
  tar -xzvf "$FILE"
}

create_kafka_topic() {
    local count=`kafka-topics.sh --describe --zookeeper "$ZK_CONNECTIONS"/kafka --topic $TOPIC 2>/dev/null | grep -c $TOPIC`
    if [[ "$count" = "0" ]];
    then
        kafka-topics.sh --create --zookeeper "$ZK_CONNECTIONS"/kafka --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC
    else
        echo "Kafka topic $TOPIC already exists"
    fi
}

run() {
  OPERATION=$1
  if [ "SETUP" = "$OPERATION" ];
  then

    echo 'kafka.brokers:' > $CONF_FILE
    echo '    - "'$KAFKA_HOST'"' >> $CONF_FILE
    echo 'kafka.port: 21005' >> $CONF_FILE
    echo 'kafka.topic: "'$TOPIC'"' >> $CONF_FILE
    echo 'kafka.partitions: '$PARTITIONS >> $CONF_FILE
    echo 'kafka.zookeeper.path: /kafka' >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'akka.zookeeper.path: /akkaQuery' >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'zookeeper.servers:' >> $CONF_FILE
    echo '    - "'$ZK_HOST'"' >> $CONF_FILE
    echo 'zookeeper.port: '$ZK_PORT >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'redis.host: "localhost"' >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'process.hosts: 1' >> $CONF_FILE
    echo 'process.cores: 4' >> $CONF_FILE
    echo >> $CONF_FILE
    echo '#Flink Specific' >> $CONF_FILE
    echo 'group.id: "flink_yahoo_benchmark"' >> $CONF_FILE
    echo 'flink.checkpoint.interval: 60000' >> $CONF_FILE
    echo 'add.result.sink: 1' >> $CONF_FILE
    echo 'flink.highcard.checkpointURI: "file:///tmp/checkpoints"' >> $CONF_FILE
    echo 'redis.threads: 20' >> $CONF_FILE
    echo >> $CONF_FILE
    echo '#EventGenerator' >> $CONF_FILE
    echo 'use.local.event.generator: 1' >> $CONF_FILE
    echo 'redis.flush: 1' >> $CONF_FILE
    echo 'redis.db: 0' >> $CONF_FILE
    echo 'load.target.hz: '$LOAD >> $CONF_FILE
    echo 'num.campaigns: 1000000' >> $CONF_FILE

    #$MVN clean install -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION"

  elif [ "START_REDIS" = "$OPERATION" ];
  then
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    stop_if_needed redis-server Redis
    rm -f "$REDIS_DIR/dump.rdb"
  elif [ "START_KAFKA" = "$OPERATION" ];
  then
    create_kafka_topic
  elif [ "STOP_KAFKA" = "$OPERATION" ];
  then
    # remove data
    echo ""
  elif [ "START_LOAD" = "$OPERATION" ];
  then
    flink run -c flink.benchmark.generator.AdImpressionsGenerator ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar $CONF_FILEf &
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    FLINK_ID=`flink list | grep 'Data Generator' | awk '{print $4}'; true`
    if [ "$FLINK_ID" == "" ];
    then
      echo "Could not find data generator to kill"
    else
      flink cancel $FLINK_ID
      sleep 3
    fi
  elif [ "START_FLINK_PROCESSING" = "$OPERATION" ];
  then
    flink run -c flink.benchmark.AdvertisingTopologyNative ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar $CONF_FILE &
    sleep 3
  elif [ "STOP_FLINK_PROCESSING" = "$OPERATION" ];
  then
    FLINK_ID=`flink list | grep 'Flink Streaming Job' | awk '{print $4}'; true`
    if [ "$FLINK_ID" == "" ];
	then
	  echo "Could not find streaming job to kill"
    else
      flink cancel $FLINK_ID
      sleep 3
    fi
  elif [ "FLINK_TEST" = "$OPERATION" ];
  then
    run "START_REDIS"
    run "START_KAFKA"
    run "START_FLINK_PROCESSING"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_KAFKA"
    run "STOP_REDIS"
  else
    if [ "HELP" != "$OPERATION" ];
    then
      echo "UNKOWN OPERATION '$OPERATION'"
      echo
    fi
    echo "Supported Operations:"
    echo "START_REDIS: run a redis instance in the background"
    echo "STOP_REDIS: kill the redis instance"
    echo "START_LOAD: run kafka load generation"
    echo "STOP_LOAD: kill kafka load generation"
    echo
    echo "START_FLINK_PROCESSING: run the flink test processing"
    echo "STOP_FLINK_PROCESSSING: kill the flink test processing"
    echo
    echo "HELP: print out this message"
    echo
    exit 1
  fi
}

if [ $# -lt 1 ];
then
  run "HELP"
else
  while [ $# -gt 0 ];
  do
    run "$1"
    shift
  done
fi
