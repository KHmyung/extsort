#!/bin/bash

ulimit -n 65536

BASEDIR=/mnt/test/
num=$1

for ((i = 1; i <= ${num}; i++))
do
	mkdir -p $BASEDIR/1/$i/input
	mkdir -p $BASEDIR/1/$i/runs
	mkdir -p $BASEDIR/1/$i/output
done
