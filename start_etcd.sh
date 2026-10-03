CLUSTER="n1=http://127.0.0.1:2380,n2=http://127.0.0.1:22380,n3=http://127.0.0.1:32380"

start() { # name clientPort peerPort
  etcd --name $1 --data-dir data/etcd-$1 \
    --listen-client-urls http://127.0.0.1:$2 --advertise-client-urls http://127.0.0.1:$2 \
    --listen-peer-urls http://127.0.0.1:$3 --initial-advertise-peer-urls http://127.0.0.1:$3 \
    --initial-cluster $CLUSTER --initial-cluster-state new > data/etcd-$1.log 2>&1 &
}

start n1 2379 2380
start n2 22379 22380
start n3 32379 32380
