MYSQL=$HOME/mysql-8.4
SEEDS="127.0.0.1:33061,127.0.0.1:33062,127.0.0.1:33063"

start() { # name clientPort serverId groupPort
  nohup $MYSQL/bin/mysqld --no-defaults --basedir=$MYSQL --datadir=$PWD/mysqldata/$1 \
    --port=$2 --socket=$PWD/mysqldata/$1.sock --mysqlx=OFF \
    --server-id=$3 --gtid-mode=ON --enforce-gtid-consistency=ON \
    --super-read-only=ON \
    --report-host=127.0.0.1 --report-port=$2 \
    --plugin-load-add=group_replication.so \
    --group-replication-group-name=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa \
    --group-replication-start-on-boot=OFF \
    --group-replication-local-address=127.0.0.1:$4 \
    --group-replication-group-seeds=$SEEDS \
    --group-replication-recovery-get-public-key=ON \
    --log-error=$PWD/mysqldata/$1.log > /dev/null 2>&1 &
}

start s1 3306 1 33061
start s2 3307 2 33062
start s3 3308 3 33063
