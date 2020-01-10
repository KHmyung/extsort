#!/bin/bash

ulimit -n 65536

BASEDIR=../

echo "(CREATE: Please enter #n)"
read num

for ((i = 1; i <= num; i++))
do
	mkdir -p $BASEDIR/input/$i
	mkdir -p $BASEDIR/runs/$i
	mkdir -p $BASEDIR/output/$i
done
