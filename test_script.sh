#!/bin/bash

ulimit -n 65536

set -e

testname=$1
testidx=0
data=new	# new or reuse
totalsize="((int64_t)1024*1024*1024*1024)"
memsize="((int64_t)128*1024*1024*1024)"
datasize=128
basedir=/home/kh/coding/2-step-sort
logdir=${basedir}/mc_result
dir0=/mnt/sorting0
dev0=nvme0n1
dir1=/mnt/sorting0
dev1=nvme0n1


init_device(){		
	if [ "$dir0" != "$dir1" ]; then
		if [ "$1" != "datagen" ]; then
			echo "Migrating input file"
			mv $dir0/input.txt $dir1/.
		fi
	fi
	echo "Initializeing ${dev0} : ${dir0} (entered with $1)"
	umount $dir0 
	nvme format /dev/$dev0 -s 1
	mkfs.btrfs /dev/$dev0
	mount /dev/$dev0 $dir0
	mkdir $dir0/runs
	mkdir $dir0/output
	sleep 1


	if [ "$dir0" != "$dir1" ]; then
		if [ "$1" != "datagen" ]; then
			echo "Loading input file"
			mv $dir1/input.txt $dir0/.
		fi
	fi

	if [ "$dir0" != "$dir1" ]; then
		echo "Initializeing ${dev1} : ${dir1}"
		umount $dir1
		nvme format /dev/$dev1 -s 1
		mkfs.ext4 /dev/$dev1
		mount /dev/$dev1 $dir1
		mkdir $dir1/runs
		mkdir $dir1/output
		sleep 1
	fi
}

set_config(){
	if [ "$1" = "datagen" ]; then
		echo "Modifying Datasize for data generation"
		sed -i 's/DO_DATAGEN false/DO_DATAGEN true/g' external_sort.h
		
		sed -i '/define TOTAL_DATA_SIZE/d' external_sort.h
		sed -i -e '27i'\#define' 'TOTAL_DATA_SIZE' ('${totalsize}')\'  external_sort.h
		sed -i '/define MEM_SIZE/d' external_sort.h
		sed -i -e '28i'\#define' 'MEM_SIZE' ('${memsize}')\'  external_sort.h
		sed -i '/define DATA_SIZE/d' external_sort.h
		sed -i -e '43i'\#define' 'DATA_SIZE' ('${datasize}')\'  external_sort.h
	else
		sed -i 's/DO_DATAGEN true/DO_DATAGEN false/g' external_sort.h
	fi


	echo "Configure I/O Path" 	
	sed -i '/define INPUT_PATH/d' external_sort.h
	sed -i -e '24i'\#define' 'INPUT_PATH' "'${dir0}/input.txt'"\'  external_sort.h
	sed -i '/define OUTPUT_PATH/d' external_sort.h
	sed -i -e '25i'\#define' 'OUTPUT_PATH' "'${dir0}/output/'"\'  external_sort.h
	sed -i '/define RUN_PATH/d' external_sort.h
	sed -i -e '26i'\#define' 'RUN_PATH' "'${dir0}/runs/'"\'  external_sort.h

}

drop_cache(){
	echo "Drop caches"
	echo 3 > /proc/sys/vm/drop_caches
}

start_log(){
	echo "Start logging ${1} on background"

	iostat 1 | grep ${dev0} > ${logdir}/[${testidx}]iops_${dev0}.log &
#	if [ "$dir0" != "$dir1" ]; then
#		iostat 1 | grep ${dev1} > ${logdir}/[$testidx]${1}th_${dev1}.log &
#	fi
	top -b | grep ocmsort > ${logdir}/[${testidx}]cpu.log &
	sleep 1
}

end_log(){
	killall -9 iostat top

	echo "Finishing this loop"
       	mv ${logdir}/*.log ${logdir}/${testname}/.
	sleep 1
}

run_test(){
	drop_cache

	sed -i '/define NRTH_RUNFORM/d' external_sort.h
	sed -i -e '38i'\#define' 'NRTH_RUNFORM' '$1'\'  external_sort.h
	sed -i '/define NRTH_RANGEMRG/d' external_sort.h
	sed -i -e '39i'\#define' 'NRTH_RANGEMRG' '$1'\'  external_sort.h

	make # build

	start_log "$1"
	echo "Start $(TZ=KST+15 date)"
	./ocmsort2 > ${logdir}/[${testidx}]result.log
	testidx=$(($testidx+1))
	sed -i 's/DO_DATAGEN true/DO_DATAGEN false/g' external_sort.h
	end_log
	rm $dir0/output/*
	#sleep 300
}

mkdir ${logdir}/${testname}

if [ "$data" = "new" ]; then
	init_device "datagen"
	set_config "datagen"
else
	set_config "nodatagen"
fi

THREAD="256 256 256 256 256 256 256 256 256 256"
for T in $THREAD
do
	echo "Thread $T"
	run_test "$T"
done


