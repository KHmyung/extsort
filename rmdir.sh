#!/bin/bash

ulimit -n 65536

BASEDIR=../

echo "(REMOVE : Please enter #n)"
read num

for ((i = 1; i <= num; i++))
do
	rm -r $BASEDIR/input/$i
	rm -r $BASEDIR/runs/$i
	rm -r $BASEDIR/output/$i
done
