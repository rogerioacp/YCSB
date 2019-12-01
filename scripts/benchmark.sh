#!/usr/bin/env bash

#set -v

#SYSTEMS=(PLAINTEXT BASELINE OBLIVPG)
SYSTEMS=(PLAINTEXT)
#SYSTEMS=(BASELINE)
#the record count is to the power of 2, e.g.: 2^7, 2^8,...
T_SIZE=(10 12 14 16 18 20 22)
# real values to be defined
T_BLOCKS=(200 450 2800 10800 42800 167800 675000)
I_BLOCKS=(10 20 100 330 1400 5280 20500)

# Max execution time. Timeout after the specified time.
# 1020s = 20min = 5min ramp-up + 15min execution time
MAX_EXEC_TIME=1200
#MAX_EXEC_TIME=300

NRUNS=1

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

    cp $TEMPLATES_PATH/workloada $YCSB_PATH/workloads/workloada
    cp $TEMPLATES_PATH/postgrenosql.properties $YCSB_PATH/postgrenosql.properties

    sed -ie "s/{tsize}/$table_size/g" $YCSB_PATH/workloads/workloada

    if [ $op == "run" ];then
        echo "maxexecutiontime=$MAX_EXEC_TIME" >> $YCSB_PATH/workloads/workloada
        echo "postgrenosql.execution=$system" >> $YCSB_PATH/postgrenosql.properties
    fi
}

function pgs_init {
    local system=$1
    local table_size=$2
    local t_nblocks=$3
    local i_nblocks=$4

    if [ $op == "load" ];then
        ssh -t -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./initdb.sh"
        cp $TEMPLATES_PATH/setup.sql setup.sql
    else
        local backup=PLAINTEXT_${table_size}_1
        ssh -t -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;cp -r backups/$backup data "
        ssh -t -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./initdb_run.sh"
        cp $TEMPLATES_PATH/setup_run.sql setup.sql
    fi

    sed -ie "s/{T_NBLOCKS}/$t_nblocks/g" setup.sql
    sed -ie "s/{I_NBLOCKS}/$i_nblocks/g" setup.sql

    psql -h $HOST -U gsd -f setup.sql test

}

function dstat_start {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local path="$PGS_PATH/${system}_${table_size}_${run}_${op}"

    screen -S DSTAT -d -m ssh -i $SSH_KEY gsd@$HOST "nohup dstat -tcdnm --fs --noheaders --output $path.csv &> ${path}_output.log &"
}

function exec_ycsb {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local path="${system}_${table_size}_${run}_${op}.dat"

    echo "ycsb $op $system $table_size $run"
    ./bin/ycsb -s $op postgrenosql -P workloads/workloada -P postgrenosql.properties &> $RESULTS_PATH/ycsb/$path

    if [ $op == "load" ] && test -f "INSERT.hdr"; then
        cp INSERT.hdr $RESULTS_PATH/ycsb/$path.hdr
    fi

    if [ $op == "run" ] && test -f "READ.hdr"; then
        cp READ.hdr $RESULTS_PATH/ycsb/$path.hdr
    fi

}

function dstat_stop {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4

    local path="$PGS_PATH/${system}_${table_size}_${run}_${op}"

    ssh -i $SSH_KEY gsd@$HOST "pkill dstat"
    scp -i $SSH_KEY gsd@$HOST:$path.csv $RESULTS_PATH/dstat/

    ssh -i $SSH_KEY gsd@$HOST "rm $path.csv"
    ssh -i $SSH_KEY gsd@$HOST "rm ${path}_output.log"
    pkill screen

}

function pgs_stop {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4

    local path="${table_size}_${run}"

    ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;./stopdb.sh"

    if [ $op == "load" ]; then
       ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH;cp -r data  backups/$path"
    fi
    ssh -i $SSH_KEY gsd@$HOST "cd $PGS_PATH; rm -rf data"


}


function run_test {
    local system=$1
    local table_size=$2
    local run=$3
    local op=$4
    local t_blocks=$5
    local i_blocks=$6

    ycsb_init $table_size $op $system
    pgs_init $system $table_size $t_blocks $i_blocks
    dstat_start $system $table_size $run $op
    exec_ycsb $system $table_size $run $op
    dstat_stop $system $table_size $run $op
    pgs_stop $system $table_size $run $op
}


for system in ${SYSTEMS[@]};
do
    size="${#T_SIZE[@]}"
    size=$((size - 1))
    echo "Tsize is $size"

    #for exponent in ${T_SIZE[@]};
    for i in $(seq 0 ${size});
    do
        for j in $(seq 1 $NRUNS);
        do
            exponent=${T_SIZE[i]}
            t_blocks=${T_BLOCKS[i]}
            i_blocks=${I_BLOCKS[i]}
            table_size=$((2**($exponent+0)))
            echo "$(date) - Run $j for $system with table size $table_size."
            run_test $system $table_size $j "load" $t_blocks $i_blocks
        done
    done
done

