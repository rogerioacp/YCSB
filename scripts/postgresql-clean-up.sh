#!/bin/bash

HOST=$(hostname)
HOME="/home/hduser"
VOL_LOCAL="local"
VOL_DFS="dfs"

POSTGRESQL_VERSION=11.3

DB=ycsbdb
TABLE=usertable

SQL_SCRIPTS_DIR="$HOME/hdfs-playbook/benchmarking/postgresql/sql-scripts"

function start_postgres_cluster {
    # Create a database cluster
    initdb -D $HOME/$VOL_LOCAL/pgsql/data
}

function start_postgres {
    # Start the database server
    postgres -D $HOME/$VOL_LOCAL/pgsql/data >logfile 2>&1 &
}

function stop_postgres {
    pg_ctl -D $HOME/$VOL_LOCAL/pgsql/data stop
}

function create_database {
    createdb $DB
}

function remove_database {
    dropdb $DB
}

function create_table {
    echo "postgresql-clean-up: create_table"
    psql -U hduser -d $DB -a -f $SQL_SCRIPTS_DIR/create-table.sql
}

function drop_table {
    echo "postgresql-clean-up: drop_table"
    psql -U hduser -d $DB -a -f $SQL_SCRIPTS_DIR/clean-table.sql
}

"$@"