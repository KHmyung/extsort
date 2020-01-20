#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

set -e
ulimit -n 65536

# Detect Boot Drive #
#CNT=$(lsblk | grep -c 238.5G)
CNT=$1
RES=$(lsblk | grep 447.1G | grep disk | awk '{print $1}')
BOOTDEV="/dev/""$RES"
DEV="/dev/sd"
BASE="/mnt/test/a/"
FS="xfs"
N_RAID=$1
DEV_RAID="md0"
CMD_RAID="mdadm -C /dev/$DEV_RAID -l 0 -n ""$CNT"

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
	echo -e "*Creating layout for test ($DEV_RAID)\n"
	sudo mkfs.$FS -f "/dev/"$DEV_RAID >> res_format.out
	mkdir -p $BASE
	sudo mount "/dev/"$DEV_RAID $BASE
	sleep 1
	
	sudo ./mkdir.sh $CNT
}

echo -e "Create raid drives with $CNT devices"
sleep 3

for x in {a..u}; do
	if [ "$N_RAID" -eq 0 ]; then
		break;
	fi
	if [ "$DEV""$x" == $BOOTDEV ]; then 
		echo -e "******* Skip Boot device($BOOTDEV) *******\n"
	else
		umount_all $x
		init_device $x
		echo "$DEV$x"
		CMD_RAID+=" $DEV$x"
		((N_RAID--))
	fi
done

sleep 1
$CMD_RAID
create_layout
