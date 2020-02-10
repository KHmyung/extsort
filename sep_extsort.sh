#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

set -e
ulimit -n 65536

RESPATH="../res/"
LOC="."
SIZE=128 #GB
MEM=16384 #MB
BASEPATH="/mnt/test/"

mkdir -p $RESPATH

x=$1	# number of threads

if [ "$*" == "" ]; then
	echo "Check argument"
	exit 1
fi

echo -e "THREAD: $x"

echo -e "Format devices"
#sudo ./format.sh $x
sudo umount /dev/nvme0n1 || /bin/true
#sudo nvme format /dev/nvme0n1 -s 1
sudo mkfs.xfs -f /dev/nvme0n1
sudo mount /dev/nvme0n1 $BASEPATH/1

sudo ./mkdir.sh $x

echo 3 > /proc/sys/vm/drop_caches

echo "sudo $LOC/extsort -G -R -M -P -C 1 -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/SEP_TH$x.log"
sudo $LOC/extsort -G -R -M -P -C 1 -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/SEP_TH$x.log

sudo ./rmdir.sh	
sleep 5
