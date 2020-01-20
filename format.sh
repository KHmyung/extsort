#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

set -e

# Detect Boot Drive #
#CNT=$(lsblk | grep -c 238.5G)
CNT=$1
RES=$(lsblk | grep 447.1G | grep disk | awk '{print $1}')
BOOTDEV="/dev/""$RES"
DEV="/dev/sd"
BASE="/mnt/test/"
FS="xfs"

umount_all(){
	echo -e "Unmount if mounted"
	sudo umount $DEV$1 || /bin/true
}

init_device(){
	echo -e "*Initializing with Secure Erase ($DEV""$1)"
	sudo hdparm -I $DEV$1 >> res_format.out
	sudo hdparm --user-master u --security-set-pass p $DEV$1 >> res_format.out
	sudo hdparm --user-master u --security-erase p $DEV$1 >> res_format.out
}

create_layout(){
	echo -e "*Creating layout for test ($BASE""$2)\n"
	sudo mkfs.$FS -f $DEV$1 >> res_format.out
	mkdir -p $BASE$2
	sudo mount $DEV$1 $BASE$2
	mkdir -p $BASE$2/input
	mkdir -p $BASE$2/output
	mkdir -p $BASE$2/runs
}

echo -e "\nSTART FORMAT with $CNT devices\n"

sleep 3

y=1

for x in {a..u}; do
	if [ "$DEV""$x" == $BOOTDEV ]; then 
		echo -e "******* Skip Boot device($BOOTDEV) *******\n"
		continue
	else
		umount_all $x
		init_device $x
		create_layout $x $y 
	fi
	if [ "$y" -eq "$CNT" ]; then
		break
	fi
	((y++))
done

sudo chmod -R 755 $BASE
