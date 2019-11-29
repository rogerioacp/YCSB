#!/bin/bash

HOST=$(hostname)
HOME="/home/hduser"

VOL_LOCAL="local"
VOL_DFS="dfs"

HBASE_PATH=$HOME/$VOL_DFS/hbase
YCSB_PATH=$HOME/$VOL_DFS/YCSB

# Deployment (hbase20 | jdbc | rocksdb)
#deployment="hbase20"

# Classpath of each deployment
# hbase_classpath=$YCSB_PATH/conf/
# jdbc_classpath=$YCSB_PATH/jdbc/lib/postgresql-42.2.5.jar
#rocksdb_dir=$HOME/$VOL_LOCAL/rocksdb

# Table
#baseline table, oblivious table (foreign table wrapper)
tables=(usertable ftw_usertable)
# Column families
#colfamilies=(col-fam1)

# Micro workloads
#micro_workloads_operation=(read)
#micro_workloads_distribution=(zipfian uniform sequential)

# Micro workloads file name
#micro_workloads_file=workloadmicro

# Macro workloads
macro_workloads=(workloada)

# Number of runs
runs=3

# Number of records for the load phase
#recordcount=12500000
## Number of operations for the run phase
operationcount=10000000
# Insert start for workloads D and E
#insertstart=12500001
# Max execution time. Timeout after the specified time.
# 1020s = 17min = 2min ramp-up + 15min execution time
maxexecutiontime=1020
# Number of worker threads
threads=1
loading_threads=25

# Time for cool down and wait for compaction to finish
# Hadoop stack
#cool_down_time_load=1680s
#cool_down_time_snapshot=180s
#cool_down_time_clean_up=150s

# Postgresql
jdbc_cool_down_time_load=60s

# RocksDB
#rocksdb_cool_down_time_load=60s

# Load interference (y | n)
#load_interference="n"

# Logging message
LOG="--- ycsb-run.sh >>"

# declare -a hosts
# readarray -t hosts < "$HOME/hdfs-playbook/deployment/hadoop-hdfs/conf-files/hosts"
# MASTER="cloud${hosts[0]}"

# cloud98
# host="192.168.115.85"
# cloud99
# host="192.168.115.212"
# cloud103
# host="192.168.115.86"


# $1 = workload file
function schema_setup {
    echo "$LOG schema_setup: $1"

        sed -i "/^table/ d"             "$YCSB_PATH/workloads/$1"
        sed -i "/^columnfamily/ d"      "$YCSB_PATH/workloads/$1"
        sed -i "/^maxexecutiontime/ d"  "$YCSB_PATH/workloads/$1"

        echo ""                                     >>  $YCSB_PATH/workloads/$1
        echo "maxexecutiontime=$maxexecutiontime"   >>  $YCSB_PATH/workloads/$1

    if [ "$deployment" = "hbase20" ] || [ "$deployment" = "rocksdb" ]; then
        echo "table=$table" >> $YCSB_PATH/workloads/$1

        for (( colfamily=0; colfamily<${#colfamilies[@]}; colfamily++ )); do
            echo "columnfamily=${colfamilies[$colfamily]}" >> $YCSB_PATH/workloads/$1
        done
    fi
}

# $1 = micro | macro
# $2 = workload file
# $3 = operation (read | insert | scan | update)
# $4 = distribution
function workload_setup {
    echo "$LOG workload-setup: $1:$2:$3:$4"

    sed -i "/^recordcount/ d"           "$YCSB_PATH/workloads/$2"
    sed -i "/^operationcount/ d"        "$YCSB_PATH/workloads/$2"
    sed -i "/^insertstart/ d"           "$YCSB_PATH/workloads/$2"

    echo ""                                 >>  $YCSB_PATH/workloads/$2
    echo "recordcount=$recordcount"         >>  $YCSB_PATH/workloads/$2
    echo "operationcount=$operationcount"   >>  $YCSB_PATH/workloads/$2

    if [ "$2" = "workloadd" ]; then
        echo "insertstart=$insertstart" >> $YCSB_PATH/workloads/$2
    fi


    if [ "$1" = "micro" ]; then
        sed -i "/^requestdistribution/ d"   "$YCSB_PATH/workloads/$2"
        echo "requestdistribution=$4"       >>  $YCSB_PATH/workloads/$2

        sed -i "/^readproportion/ d"    "$YCSB_PATH/workloads/$2"
        sed -i "/^insertproportion/ d"  "$YCSB_PATH/workloads/$2"
        sed -i "/^scanproportion/ d"    "$YCSB_PATH/workloads/$2"
        sed -i "/^updateproportion/ d"  "$YCSB_PATH/workloads/$2"

        if [ "$3" = "read" ]; then
            echo "readproportion=1"    >>  $YCSB_PATH/workloads/$2
            echo "insertproportion=0"  >>  $YCSB_PATH/workloads/$2
            echo "scanproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "updateproportion=0"  >>  $YCSB_PATH/workloads/$2
        elif [ "$3" = "insert" ]; then
            echo "readproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "insertproportion=1"  >>  $YCSB_PATH/workloads/$2
            echo "scanproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "updateproportion=0"  >>  $YCSB_PATH/workloads/$2
        elif [ "$3" = "scan" ]; then
            echo "readproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "insertproportion=0"  >>  $YCSB_PATH/workloads/$2
            echo "scanproportion=1"    >>  $YCSB_PATH/workloads/$2
            echo "updateproportion=0"  >>  $YCSB_PATH/workloads/$2
        elif [ "$3" = "update" ]; then
            echo "readproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "insertproportion=0"  >>  $YCSB_PATH/workloads/$2
            echo "scanproportion=0"    >>  $YCSB_PATH/workloads/$2
            echo "updateproportion=1"  >>  $YCSB_PATH/workloads/$2
        fi
    fi

}

# $1 = micro | macro
# $2 = workload file
# $3 = run
function setup_ycsb_result_file {
    echo "$LOG setup_ycsb_result_file: $1:$2:$3"

    sed -i "/^exportfile/ d"            "$YCSB_PATH/workloads/$2"

    echo "exportfile=$HOME/$VOL_DFS/ycsb-results/$1/$deployment/$2/thr-$threads/ycsb-throughput-$3" >> "$YCSB_PATH/workloads/$2"

}

# Launch dstat in each node
# $1 = workload (micro | macro)
# $2 = workload file
# $3 = run
# $5 = operation (read | insert | scan | update | null)
function dstat_launch {
    echo "$LOG dstat-launch: $1:$2:$3"

    # hlen=${#hosts[@]}

    # for (( host=0; host<${hlen}; host++ )); do
    #     if [ "$1" = "micro" ]; then
    #         screen -S cloud${hosts[$host]} -d -m ssh cloud${hosts[$host]} "dstat -tcdnm --fs -D sda6 --noheaders --output $HOME/$VOL_DFS/dstat-$deployment-$deployment-$2-$5-$threads-$3.csv"
    #     else
    #         screen -S cloud${hosts[$host]} -d -m ssh cloud${hosts[$host]} "dstat -tcdnm --fs -D sda6 --noheaders --output $HOME/$VOL_DFS/dstat-$deployment-$2-$threads-$3.csv"
    #     fi

    # done

    screen -S $HOST -d -m ssh $host "dstat -tcdnm --fs -D sda6 --noheaders --output $HOME/$VOL_DFS/dstat-$deployment-$2-$threads-$3.csv"
}

# Stop dstat in each node
# $1 = workload (micro | macro)
# $2 = workload file
# $3 = run
# $5 = operation (read | insert | scan | update | null)
function dstat_stop {
    echo "$LOG dstat-stop: $1:$2:$3:$4"

    # # Collect the dstat results of each node
    # hlen=${#hosts[@]}
    # for (( hostCR=0; hostCR<${hlen}; hostCR++ )); do
    #     screen -X -S cloud${hosts[$hostCR]} quit

    #     if [ "$1" = "micro" ]; then
    #         scp hduser@cloud${hosts[$hostCR]}:$HOME/$VOL_DFS/dstat-$deployment-$2-$4-$threads-$3.csv $HOME/$VOL_DFS/ycsb-results/$1/$deployment/thr-$threads/$5/dstat-$deployment-$2-$5-$threads-$3-cloud${hosts[$hostCR]}.csv
    #     else
    #         scp hduser@cloud${hosts[$hostCR]}:$HOME/$VOL_DFS/dstat-$deployment-$2-$threads-$3.csv $HOME/$VOL_DFS/ycsb-results/$1/$deployment/$2/thr-$threads/dstat-$deployment-$2-$threads-$3-cloud${hosts[$hostCR]}.csv
    #     fi

    # done

    screen -X -S $HOST quit

    scp hduser@$host:$HOME/$VOL_DFS/dstat-$deployment-$2-$threads-$3.csv $HOME/$VOL_DFS/ycsb-results/$1/$deployment/$2/thr-$threads/dstat-$deployment-$2-$threads-$3.csv

    rm $HOME/$VOL_DFS/dstat-$deployment-$2-$threads-$3.csv
}

# Run micro workloads
# Outdated, need to add workers threads
function run_micro {
    echo "$LOG run_micro"

    for operation in ${micro_workloads_operation[@]}; do
        echo "- $operation"

        for distribution in ${micro_workloads_distribution[@]}; do
            echo "-- $distribution"

            mkdir -p $HOME/$VOL_DFS/ycsb-results/micro/$micro_workloads_file/$operation/$distribution/

            # setup schema for workload-micro file
            schema_setup $micro_workloads_file
            # setup workload for workload-micro file
            workload_setup micro $micro_workloads_file $operation $distribution

            for (( run=1; run<($runs+1); run++ )); do
                echo "--- $run"
                # start dstat
                dstat_launch micro $micro_workloads_file $distribution $run $operation

                # run ycsb
                /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/micro/$micro_workloads_file/$operation/$distribution/time-$run    $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$micro_workloads_file -cp '$HOME/$VOL_DFS/YCSB/conf/' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/micro/$micro_workloads_file/$operation/$distribution/throughput-$run.dat

                # stop dstat
                dstat_stop micro $micro_workloads_file $distribution $run $operation

                if [ "$operation" = "insert" ] || [ "$operation" = "update" ]; then
                    # restore snapshot
                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hbase-clean-up.sh restore_snapshot $table $table-$deployment

                    # wait for async hbase operations to finish
                    sleep $wtime
                fi

            done

        done

    done

}


# Run micro workloads
# $1 = workload
function run_macro {
    echo "$LOG run_macro"

    # for workload in ${macro_workloads[@]}; do
    #     echo "- $workload"

        mkdir -p $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads

        # setup schema for workload-macro file
        schema_setup $1
        # setup workload for workload-macro file
        workload_setup macro $1

        cd $YCSB_PATH

        for (( run=1; run<($runs+1); run++ )); do
            echo "$LOG run-$run"
            # setup path to ycsb report file
            setup_ycsb_result_file macro $1 $run

            # start dstat
            dstat_launch macro $1 $run

            # run ycsb
            if [ "$deployment" = "hbase20" ]; then
                echo "$LOG $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -cp '/home/hduser/dfs/YCSB/conf' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat"

                /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/time-$run    $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -cp '/home/hduser/dfs/YCSB/conf' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat

                # stop dstat
                dstat_stop macro $1 $run

                # Condition of load interference testing scenario
                if [ "$load_interference" = "y" ]; then
                    # Hadoop general clean-up
                    echo "$LOG hadoop general clean-up"
                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hadoop-stack-clean-up.sh clean_all format $table $table-$deployment col-fam1 $recordcount

                    echo "$LOG cool donw from greneral clean-up"
                    sleep $cool_down_time_clean_up

                    # YCSB loading phase
                    echo "$LOG YCSB loading phase"
                    $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment macro $run

                    # # setup schema for workload-macro file
                    # schema_setup $1
                    # # setup workload for workload-macro file
                    # workload_setup macro $1

                else
                    # Flush HBase memstore
                    echo "$LOG flush HBase memstore"
                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hbase-clean-up.sh hbase_flush_table $table

                    # Restore snapshot
                    echo "$LOG restore snapshot"
                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hbase-clean-up.sh restore_snapshot $table $table-$deployment

                    # Cooldown from snapshot
                    echo "$LOG cooldown from snapshot :$cool_down_time_snapshot"
                    sleep $cool_down_time_snapshot

                    # Clean local cache
                    echo "$LOG clean local cache"
                    $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

                    # Restart Hadoop
                    echo "$LOG Restart Hadoop"
                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hadoop-stack-clean-up.sh stop_hadoop_stack

                    $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hadoop-stack-clean-up.sh start_hadoop_stack

                    sleep 30s
                fi

            elif [ "$deployment" = "jdbc" ]; then
                echo "$LOG $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -cp '/home/hduser/dfs/YCSB/jdbc/lib/postgresql-42.2.5.jar' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat"

                /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/time-$run    $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -cp '/home/hduser/dfs/YCSB/jdbc/lib/postgresql-42.2.5.jar' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat

                # stop dstat
                dstat_stop macro $1 $run

                # # Stop postgres
                # echo "$LOG stop postgresql"
                # $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh stop_postgres

                # # Clean local cache
                # echo "$LOG clean local cache"
                # $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

                # # Start postgres
                # echo "$LOG start postgresql"
                # $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh start_postgres

                # Drop postgresql database
                echo "$LOG drop postgresql database"
                $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh remove_database

                # Clean local cache
                echo "$LOG clean local caches"
                $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

                # Create postgresql database
                echo "$LOG create postgresql database"
                $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh create_database

                # Create table
                echo "$LOG postgresql create table"
                $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh create_table

                # YCSB loading-phase
                echo "$LOG YCSB loading phase"
                $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment macro 0

                # Loading-phase cooldown time
                echo "$LOG postgresql loading-phase cooldown time"
                sleep $jdbc_cool_down_time_load

                # Stop postgres
                echo "$LOG postgresql stop postgresql"
                $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh stop_postgres

                # Clean local cache
                echo "$LOG clean local cache"
                $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

                # Start postgres
                echo "$LOG start postgresql"
                $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh start_postgres


            elif [ "$deployment" = "rocksdb" ]; then
                echo "$LOG $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -p rocksdb.dir='$rocksdb_dir' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat"

                /usr/bin/time --verbose --output=$HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/time-$run    $YCSB_PATH/bin/ycsb run $deployment -P $YCSB_PATH/workloads/$1 -p rocksdb.dir='/home/hduser/local/rocksdb' -s -threads $threads &> $HOME/$VOL_DFS/ycsb-results/macro/$deployment/$1/thr-$threads/ycsb-runtime-throughput-$run.dat

                # stop dstat
                dstat_stop macro $1 $run

                # Execution-phase cooldown time
                echo "$LOG Cooldown time after execution"
                sleep $rocksdb_cool_down_time_load

                # Clean local cache
                echo "$LOG clean local cache"
                $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

                # Remove database contents
                echo "$LOG remove database contents"
                rm $HOME/$VOL_LOCAL/rocksdb/*

                # YCSB loading-phase
                echo "$LOG YCSB loading phase"
                $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment macro $run

                # Loading-phase cooldown time
                echo "$LOG RocksDB cool down time from YCSB load"
                sleep $rocksdb_cool_down_time_load

                # Clean local cache
                echo "$LOG RocksDB clean local cache"
                $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

            fi

            # setup schema for workload-macro file
            schema_setup $1
            # setup workload for workload-macro file
            workload_setup macro $1

        done

        # After starting a new workload
        # if [ "$deployment" = "jdbc" ]; then
        #     # Drop postgresql database
        #     echo "$LOG drop postgresql database"
        #     $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh remove_database

        #     # Clean local cache
        #     echo "$LOG clean local caches"
        #     $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

        #     # Create postgresql database
        #     echo "$LOG create postgresql database"
        #     $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh create_database

        #     # Create table
        #     echo "$LOG postgresql create table"
        #     $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh create_table

        #     # YCSB loading-phase
        #     echo "$LOG YCSB loading phase"
        #     $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment macro 0

        #     # Loading-phase cooldown time
        #     echo "$LOG postgresql loading-phase cooldown time"
        #     sleep $jdbc_cool_down_time_load

        #     # Stop postgres
        #     echo "$LOG postgresql stop postgresql"
        #     $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh stop_postgres

        #     # Clean local cache
        #     echo "$LOG clean local cache"
        #     $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

        #     # Start postgres
        #     echo "$LOG start postgresql"
        #     $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh start_postgres

        # elif [ "$deployment" = "rocksdb" ]; then
        #     # Remove database contents
        #     echo "$LOG remove database contents"
        #     rm $HOME/$VOL_LOCAL/rocksdb/*

        #     # YCSB loading-phase
        #     echo "$LOG YCSB loading phase"
        #     $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment macro 0

        #     # Loading-phase cooldown time
        #     echo "$LOG RocksDB cool down time from YCSB load"
        #     sleep $rocksdb_cool_down_time_load

        #     # Clean local cache
        #     echo "$LOG RocksDB clean local cache"
        #     $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

        # fi

    # done

}


# $1 = workload file
# $2 = testbed (micro | macro)
function load_phase {
    echo "$LOG load_phase : $1:$2"

    $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh load $1 $recordcount $deployment $2 0

    if [ "$load_interference" = "n" ]; then
        if [ "$deployment" = "hbase20" ]; then
            # Loading-phase cooldown time
            echo "$LOG Loading-phase cooldown time: sleep $cool_down_time_load"
            sleep $cool_down_time_load

            # Flush HBase memstore
            echo "$LOG Flush HBase memstore"
            $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hbase-clean-up.sh hbase_flush_table $table

            # Create snapshot after cooldown time
            echo "$LOG Create snapshot after cooldown time"
            $HOME/hdfs-playbook/benchmarking/drss-paper/ycsb-load.sh create_snapshot $deployment

            # Clean local cache
            echo "$LOG Clean local cache"
            $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

            # Restart Hadoop
            echo "$LOG Restart Hadoop"
            $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hadoop-stack-clean-up.sh stop_hadoop_stack

            $HOME/hdfs-playbook/benchmarking/hadoop-hdfs/pseudo-distributed/hadoop-stack-clean-up.sh start_hadoop_stack

            sleep 30s

        elif [ "$deployment" = "jdbc" ]; then
            # Stop postgres
            echo "$LOG Stop postgresql"
            $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh stop_postgres

            # Clean local cache
            echo "$LOG Clean local cache"
            $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache

            # Start postgres
            echo "$LOG Start postgresql"
            $HOME/hdfs-playbook/benchmarking/postgresql/postgresql-clean-up.sh start_postgres

            # Cooldown time after starting postgres database server
            echo "$LOG Cooldown after starting postgresql database server"
            sleep $jdbc_cool_down_time_load

        elif [ "$deployment" = "rocksdb" ]; then
            # Clean local cache
            echo "$LOG Clean local cache"
            $HOME/hdfs-playbook/benchmarking/drss-paper/general/general-clean-up.sh clean_cache
        fi
    fi


}

function load {
    load_phase workloada macro
}

function execute {
    run_macro workloada
    run_macro workloadb
    run_macro workloadc
    run_macro workloadd
    run_macro workloade
    run_macro workloadf
}

"$@"
