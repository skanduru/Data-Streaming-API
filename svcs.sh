#!/usr/bin/env bash

cur_py_args=$*
logfile=logs/dbg_$(echo $cur_pyfile | sed 's,/,_,g').log

spark="2.4.8"
case $spark in
"2.4.8")
    export PYSPARK_PYTHON=/usr/local/bin/python3
    export JNAME="2.11-2.4.8"
    export SQ_K="0-10_2.11:2.4.8"
    ;;
"2.3.4")
    export PYSPARK_PYTHON=/usr/local/bin/python3
    export JNAME="2.11-2.3.4"
    export SQ_K="0-10_2.11:2.3.4"
    ;;
"3.2.1")
    export PYSPARK_PYTHON=/usr/bin/python3
    export JNAME="2.12-3.2.1"
    export SQ_K="0-10_2.13:3.2.1"
    ;;
esac

    export SPARK_HOME=/opt/spark-$spark-bin-hadoop2.7
    export SPBIN=$SPARK_HOME/bin
    export PATH=$SPBIN:/opt/kafka/bin:/usr/lib/postgresql/12/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin
    dir="$SPARK_HOME/jars"
    jop=" --jars $dir/spark-sql-${JNAME}.jar,$dir/spark-streaming-${JNAME}.jar"
    spark-submit --packages org.apache.spark:spark-sql-kafka-${SQ_K} $jop $cur_py_args 2>&1 | tee $logfile
