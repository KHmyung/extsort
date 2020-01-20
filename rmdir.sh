#!/bin/bash

ulimit -n 65536

BASEDIR=/mnt/test/
cnt=$1

for ((i = 1; i <= ${cnt}; i++))
do
	sudo umount $BASEDIR$i || /bin/true
done
