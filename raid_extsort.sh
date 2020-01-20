#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

set -e
ulimit -n 65536

RESPATH="../res/raid"
LOC="."
SIZE=64 #GB
MEM=8192 #MB
BASEPATH="/mnt/test/a/"

mkdir -p $RESPATH

x=$1

if [ "$*" == "" ]; then
	echo "Check argument"
	exit 1
fi

echo -e "THREAD: $x"
	
echo -e "Create RAID"
sudo ./mkraid.sh $x


echo "sudo $LOC/extsort -G -R -M -P -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/RAID_TH$x.log"
sudo $LOC/extsort -G -R -M -P -b $BASEPATH -d $SIZE -m $MEM -w $x > $RESPATH/RAID_TH$x.log

echo -e "Remove RAID"
sudo ./rmraid.sh
sleep 5
