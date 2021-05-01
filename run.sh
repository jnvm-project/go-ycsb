#!/bin/bash

EXP=10
EXP_DIR="/home/yohan/results/exp${EXP}"
OPORDER=27
OPERATIONCOUNT=$(echo 2^${OPORDER} | bc -l)

mkdir -p ${EXP_DIR}

for i in {17..26}; do
	ORDER=$i
	RECORDCOUNT=$(echo 2^${ORDER} | bc -l)
	RECORDCOUNT2=$(echo $RECORDCOUNT - 100 | bc -l)
	echo $RECORDCOUNT $RECORDCOUNT2 $OPERATIONCOUNT

	VAR=$(echo "(30/(2^${ORDER}*2.4/1024/1024))*100" | bc -l)
	VARINT=$(echo "$VAR / 1" | bc)
	echo "GOGC = $VARINT"

	sudo rm /pmem0/coucou

	sudo numactl --physcpubind=0,2,4,6,8,10,12,14,16,18 --preferred=0 ./bin/go-ycsb load hpredis -p recordcount=${RECORDCOUNT} -p threadcount=24 -p insertorder=ordered | tee ${EXP_DIR}/data_hpredis_load_rec${ORDER}_op${OPORDER}
	
	sudo GOGC=${VARINT} numactl --physcpubind=0,2,4,6,8,10,12,14,16,18 --preferred=0 ./bin/go-ycsb run hpredis -P workloads/workloadf -p recordcount=${RECORDCOUNT2} -p operationcount=${OPERATIONCOUNT} -p threadcount=10 -p insertorder=ordered | tee ${EXP_DIR}/data_hpredis_run_rec${ORDER}_op${OPORDER}
done

