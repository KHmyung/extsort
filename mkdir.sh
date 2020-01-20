#!/bin/bash

ulimit -n 65536

BASEDIR=/mnt/test/a/
num=$1

for ((i = 1; i <= ${num}; i++))
do
	mkdir -p $BASEDIR$i/input
	mkdir -p $BASEDIR$i/runs
	mkdir -p $BASEDIR$i/output
done
