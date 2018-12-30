#!/bin/bash

source ~/.bash_profile
USER_NAME=$(id -un)
EXTERNAL_JAR="/Users/JerryHan88/JavaProjects/_externalJAR/"

APP_NAME="SGMnILP"
INPUT="/Users/JerryHan88/Dropbox/_researchData/scd/Experiments/test1"
OUTPUT="/Users/JerryHan88/Dropbox/_researchData/scd/Experiments/solution_$APP_NAME"
TIME_LIMIT=10800
CONFIG="/Users/JerryHan88/JavaProjects/scd_java/config.properties"

PROCESS_LOG_INTERVAL=5
JAVE_TERMINATION=12600

for fpath in $INPUT/*.json; do
	fn=$(echo $fpath | sed "s/.*\///")
	ph_log_fn=$(echo $fn | sed "s/prmt/ph/" | sed "s/json/csv/")
	echo $fn
	echo $OUTPUT/$ph_log_fn
	python -c "from processHandler import logging_java_process; logging_java_process($PROCESS_LOG_INTERVAL, '$OUTPUT/$ph_log_fn', '$JAVA_PATH')" &
    python -c "from processHandler import resv_kill_java_process; resv_kill_java_process($JAVE_TERMINATION)" &

    java -cp .:$EXTERNAL_JAR/* -Djava.library.path=$CPLEX_BINARY Main -i $fpath -o $OUTPUT -a $APP_NAME -t $TIME_LIMIT -c $CONFIG
    kill $(ps -e | grep python | awk '{print $1}')
done


#java -cp .:$EXTERNAL_JAR/* -Djava.library.path=$CPLEX_BIN Main -i $INPUT -o $OUTPUT -a $APP_NAME -t $TIME_LIMIT -c $CONFIG
