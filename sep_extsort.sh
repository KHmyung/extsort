#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

set -e
ulimit -n 65536

RESPATH="../res/sep"
LOC="."
SIZE=64 #GB
MEM=8192 #MB
BASEPATH="/mnt/test/"

mkdir -p $RESPATH

x=$1	# number of threads

if [ "$*" == "" ]; then
	echo "Check argument"
	exit 1
fi

echo -e "THREAD: $x"

echo -e "Format devices"
sudo ./format.sh $x

echo "sudo $LOC/extsort -G -R -M -P -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/SEP_TH$x.log"
sudo $LOC/extsort -G -R -M -P -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/SEP_TH$x.log

sudo ./rmdir.sh $x	
sleep 5
