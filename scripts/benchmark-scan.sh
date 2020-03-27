#!/usr/bin/env bash

#set -v

#SYSTEMS=(PLAINTEXT)
#SYSTEMS=(BASELINE)
#SYSTEMS=(OIS)
SYSTEM=BASELINE

#the record count is to the power of 2, e.g.: 2^7, 2^8,...
#T_SIZE=16
#T_BLOCK=65540
#I_BLOCK=310
T_SIZE=14
T_BLOCK=16390
I_BLOCK=80
SIZES=(10 20 30 40)
#SIZES=(10 20 30 40 50)

#T_SIZE=(4 6 8 10 12 14 16)
#T_BLOCKS=(18 68 260 1030 4100 16390 65540)
#I_BLOCKS=(4 4 6 10 24 80 310)


# Max execution time. Timeout after the specified time.
# 1020s = 20min = 5min ramp-up + 15min execution time
MAX_EXEC_TIME=2100
NRUNS=5

HOST=cloud105

TEMPLATES_PATH="templates"
YCSB_PATH="."
PGS_PATH="/home/gsd/sparknvme/postgresql"
SSH_KEY="gsd_private_key"
RESULTS_PATH="results"

export PATH=$PATH:/usr/local/pgsql/bin

function ycsb_init {
    local table_size=$1
    local op=$2
    local system=$3
    local size=$4
    local key=$((table_size -size))


    cp $TEMPLATES_PATH/workloadb $YCSB_PATH/workloads/workloadb
    cp $TEMPLATES_PATH/postgrenosql.properties $YCSB_PATH/postgrenosql.properties

    sed -ie "s/{tsize}/$table_size/g" $YCSB_PATH/workloads/workloadb
    sed -ie "s/{key}/$key/g" $YCSB_PATH/workloads/workloadb
    sed -ie "s/{qsize}/$size/g" $YCSB_PATH/workloads/workloadb

    if [ $op == "run" ];then
        echo "maxexecutiontime=$MAX_EXEC_TIME" >> $YCSB_PATH/workloads/workloadb
        echo "postgrenosql.execution=$system" >> $YCSB_PATH/postgrenosql.properties
    fi
}

function pgs_init {
    local system=$1
    local table_size=$2
    local t_nblocks=$3
    local i_nblocks=$4

    if [ $op == "load" ];then
        ssh -tt -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./initdb.sh"
        cp $TEMPLATES_PATH/setup.sql setup.sql
    else
        local backup=${table_size}_1
        echo "backup file is $backup"
        ssh -tt -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;cp -r backups/$backup data"
        ssh -tt -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;cp postgresql.conf data/postgresql.conf"
        ssh -tt -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./initdb_run.sh"
        cp $TEMPLATES_PATH/setup_run.sql setup.sql
        sed -ie "s/{T_NBLOCKS}/$t_nblocks/g" setup.sql
        sed -ie "s/{I_NBLOCKS}/$i_nblocks/g" setup.sql
    fi
    
    psql -h $HOST -U gsd -f setup.sql test
 }

function dstat_start {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local path="$PGS_PATH/${system}_${table_size}_${run}_${op}"

    ssh -i $SSH_KEY gsd@$HOST "nohup dstat -tcdnm --fs --noheaders --output $path.csv &> ${path}_output.log &"
}

function exec_ycsb {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local size=$5
    local path="${system}_${size}_${run}_${op}.dat"

    echo "ycsb $op $system $table_size $run"
    ./bin/ycsb -s $op postgrenosql -P workloads/workloadb -P postgrenosql.properties  &> $RESULTS_PATH/ycsb/$path

    if [ $op == "load" ] && test -f "INSERT.hdr"; then
        cp INSERT.hdr $RESULTS_PATH/ycsb/$path.hdr
    fi

    if [ $op == "run" ] && test -f "SCAN.hdr"; then
        cp SCAN.hdr $RESULTS_PATH/ycsb/$path.hdr
    fi

}

function dstat_stop {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4

    local path="$PGS_PATH/${system}_${table_size}_${run}_${op}"

    ssh -i $SSH_KEY gsd@$HOST "pkill python"
    scp -i $SSH_KEY gsd@$HOST:$path.csv $RESULTS_PATH/dstat/

    ssh -i $SSH_KEY gsd@$HOST "rm $path.csv"
    ssh -i $SSH_KEY gsd@$HOST "rm ${path}_output.log"
    #pkill screen

}

function pgs_stop {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4

    local path="${table_size}_${run}"

    ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./stopdb.sh"
    sleep 2
    if [ $op == "load" ]; then
       ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;cp -r data  backups/$path"
    fi
    #ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH; cp -r data/log logs/$path"
    ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH; rm -rf data"


}


function run_test {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local t_blocks=$5
    local i_blocks=$6
    local size=$7

    ycsb_init $table_size $op $system $size
    pgs_init $system $table_size $t_blocks $i_blocks
    dstat_start $system $table_size $run $op
    exec_ycsb $system $table_size $run $op $size
    dstat_stop $system $table_size $run $op
    pgs_stop $system $table_size $run $op
}


for size in ${SIZES[@]};
do
    for j in $(seq 1 $NRUNS);
    do	
        table_size=$((2**($T_SIZE+0)))
        echo "$(date) - Run $j for $SYSTEM with table size $table_size."
        run_test $SYSTEM $table_size $j "run" $T_BLOCK $I_BLOCK $size
    done
done

