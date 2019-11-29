#!/bin/bash

HOST=$(hostname)
HOME="/home/hduser"

VOL_LOCAL="local"
VOL_DFS="dfs"

HBASE_PATH=$HOME/$VOL_DFS/hbase
POSTGRES_PATH=$HOME/$VOL_DFS/postgres
LEVELDB_PATH=$HOME/$VOL_DFS/leveldb

YCSB_PATH=$HOME/$VOL_DFS/YCSB

# Classpath of each deployment
# hbase_classpath="$YCSB_PATH/conf/"
# jdbc_classpath=$YCSB_PATH/jdbc/lib/postgresql-42.2.5.jar
# rocksdb_dir=$HOME/$VOL_LOCAL/rocksdb

# HBase table
table="table-test"
# Column families
colfamilies=(col-fam1)


loading_threads=25



# declare -a hosts
# readarray -t hosts < "$HOME/hdfs-playbook/deployment/hadoop-hdfs/conf-files/hosts"
# MASTER="cloud${hosts[0]}"

# cloud98
# host="192.168.115.85"
# cloud99
# host="192.168.115.212"
# cloud103
# host="192.168.115.86"


# $1 = workload
# $2 = deployment
function schema_setup {
    echo "ycsb-load: schema_setup: $1"

    sed -i "/^table/ d"         "$YCSB_PATH/workloads/$1"
    sed -i "/^columnfamily/ d"  "$YCSB_PATH/workloads/$1"

    if [ "$2" != "jdbc" ]; then
        echo "table=$table"   >>  $YCSB_PATH/workloads/$1
        
        for (( colfamily=0; colfamily<${#colfamilies[@]}; colfamily++ )); do
            echo "columnfamily=${colfamilies[$colfamily]}" >> $YCSB_PATH/workloads/$1
        done
    fi
}

# $1 = workload
# $2 = recordcount
# $3 = operationcount
# $4 = deployment
function workload_setup {
    echo "ycsb-load:workload-setup: "

    sed -i "/^recordcount/ d"           "$YCSB_PATH/workloads/$1"
    sed -i "/^operationcount/ d"        "$YCSB_PATH/workloads/$1"
    sed -i "/^maxexecutiontime/ d"      "$YCSB_PATH/workloads/$1"
    sed -i "/^exportfile/ d"            "$YCSB_PATH/workloads/$1"
    sed -i "/^insertstart/ d"           "$YCSB_PATH/workloads/$1"

    echo "recordcount=$2"           >>  $YCSB_PATH/workloads/$1
    echo "operationcount=$3"        >>  $YCSB_PATH/workloads/$1
    
    echo "exportfile=$HOME/$VOL_DFS/ycsb-results/load/$4/$1/ycsb-load-throughput-$1" >> "$YCSB_PATH/workloads/$1"
}


# Launch dstat in each node
# $1 = testbed (micro | macro)
# $2 = workload-file
function dstat_launch {
    echo "ycsb-load:dstat-launch"
    
    # hlen=${#hosts[@]}

    # for (( host=0; host<${hlen}; host++ )); do
        # screen -S cloud${hosts[$host]} -d -m ssh cloud${hosts[$host]} "dstat -tcdnm --fs -D sda6 --noheaders --output $HOME/$VOL_DFS/dstat-load-$1-$2.csv"
    # done

    screen -S $HOST -d -m ssh $host "dstat -tcdnm --fs -D sda6 --noheaders --output $HOME/$VOL_DFS/dstat-load-$1-$2.csv"
}

# Stop dstat in each node
# $1 = testbed (micro | macro)
# $2 = workload-file
# $3 = deployment
function dstat_stop {
    echo "ycsb-load:dstat-stop"
    
    # Collect the dstat results of each node
    # hlen=${#hosts[@]}
    # for (( hostCR=0; hostCR<${hlen}; hostCR++ )); do
    #     screen -X -S cloud${hosts[$hostCR]} quit

    #     scp hduser@cloud${hosts[$hostCR]}:$HOME/$VOL_DFS/dstat-load-$1-$2.csv $HOME/$VOL_DFS/ycsb-results/load/$3/$2/dstat-load-$1-$2-cloud${hosts[$hostCR]}.csv

    #     rm $HOME/$VOL_DFS/dstat-load-$1-$2.csv
    # done

    screen -X -S $HOST quit

    scp hduser@$host:$HOME/$VOL_DFS/dstat-load-$1-$2.csv $HOME/$VOL_DFS/ycsb-results/load/$3/$2/dstat-load-$1-$2-$HOST.csv

    rm $HOME/$VOL_DFS/dstat-load-$1-$2.csv
}

# $1 = deployment
function create_snapshot {
    echo "ycsb-load: create_snapshot: $1"
    if [ "$1" = "hbase20" ]; then
        echo "snapshot '$table', '$table-$1'" | $HBASE_PATH/bin/hbase shell
    fi 

}


# $1 = workload file
# $2 = recordcount
# $3 = deployment (hbase20 jdbc rocksdb)
# $4 = testbed (micro | macro)
# $5 = run
function load {
    echo "ycsb-load: $1:$2:$3"
    mkdir -p $HOME/$VOL_DFS/ycsb-results/load/$3/$1

    schema_setup $1 $3
    workload_setup $1 $2 $2 $3 
    dstat_launch $4 $1

    cd $YCSB_PATH

    if [ "$3" = "hbase20" ]; then
        echo "bin/ycsb load $3 -P workloads/$1 -cp '/home/hduser/dfs/YCSB/conf' -s -threads $loading_threads"

        /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/load/$3/$1/time-load-$4-$1  bin/ycsb load $3 -P workloads/$1 -cp '/home/hduser/dfs/YCSB/conf' -s -threads $loading_threads &> $HOME/$VOL_DFS/ycsb-results/load/$3/$1/ycsb-load-runtime-throughput-$5.dat
    
    elif [ "$3" = "jdbc" ]; then
        echo "bin/ycsb load $3 -P workloads/$1 -cp '/home/hduser/dfs/YCSB/jdbc/lib/postgresql-42.2.5.jar' -s -threads $loading_threads"

        /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/load/$3/$1/time-load-$4-$1  bin/ycsb load $3 -P workloads/$1 -cp '/home/hduser/dfs/YCSB/jdbc/lib/postgresql-42.2.5.jar' -s -threads $loading_threads &> $HOME/$VOL_DFS/ycsb-results/load/$3/$1/ycsb-load-runtime-throughput-$5.dat

    elif [ "$3" = "rocksdb" ]; then
        echo "bin/ycsb load $3 -P workloads/$1 -p rocksdb.dir='$rocksdb_dir' -s -threads $loading_threads"

        /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/load/$3/$1/time-load-$4-$1  bin/ycsb load $3 -P workloads/$1 -p rocksdb.dir='/home/hduser/local/rocksdb' -s -threads $loading_threads &> $HOME/$VOL_DFS/ycsb-results/load/$3/$1/ycsb-load-runtime-throughput-$5.dat
    
    fi

    dstat_stop $4 $1 $3
}


"$@"