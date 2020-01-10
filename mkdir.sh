#!/bin/bash

BASEDIR=../

for x in {1..128}; do
	mkdir -p $BASEDIR/input/$x
	mkdir -p $BASEDIR/runs/$x
	mkdir -p $BASEDIR/output/$x
done
