#!/usr/bin/env bash

cd test
echo "===> Changing directory to \"./test\""

docker-compose up -d --build --force-recreate cassandra zookeeper kafka

function ping_cassandra() {
  docker exec -it cassandra /opt/bitnami/cassandra/bin/nodetool status | grep UN
  res=$?
}

echo "Waiting for Cassandra to be ready."

# For some reason, GoCql still can't connect to Cassandra even if the nodetool
# shows positive results. There has to be a better way than this.
max_attempts=40
cur_attempts=0
ping_cassandra
while (( res != 0 && ++cur_attempts != max_attempts ))
do
  ping_cassandra
  echo Attempt: $cur_attempts of $max_attempts
  sleep 1
done

if (( cur_attempts == max_attempts )); then
  echo "Cassandra Timed Out."
  exit 1
else
  echo "Cassandra response received."
fi

# The Cassandra image takes more time to be ready despite
# nodetool-status being success.
# There has to be a better way than this.
echo "Waiting additional time for Cassandra to be ready."
add_wait=40
cur_add_wait=0
while (( ++cur_add_wait != add_wait ))
do
  echo Additional Wait: $cur_add_wait of $add_wait seconds
  sleep 1
done

docker-compose up -d --build --force-recreate go-logsink

docker-compose up -d --build --force-recreate go-eventpersistence

# echo "Waiting for Go-API Sessions to initialize"
sleep 5

# Run the tests
docker-compose up --exit-code-from go-eventpersistence
rc=$?
if [[ $rc != 0 ]]
  docker ps -a
  then exit $rc
fi
