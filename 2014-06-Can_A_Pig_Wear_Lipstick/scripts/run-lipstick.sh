#!/bin/bash

export HADOOP_CLASSPATH=$CLASSPATH:/etc/hive/conf

LIPSTICK_CONSOLE_PATH=/home/matt/Lipstick/Lipstick/lipstick-console/build/libs/lipstick-console-0.6-SNAPSHOT-withPig.jar
LIPSTICK_SERVER_URL=http://cdh5.mattdavies.net:8080/lipstick

hadoop jar \
   ${LIPSTICK_CONSOLE_PATH} \
   -Dlipstick.server.url=${LIPSTICK_SERVER_URL} \
   -Dhive.metastore.uris=thrift://cdh5:9083 \
   ${@}

