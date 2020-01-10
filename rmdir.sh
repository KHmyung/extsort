#!/bin/bash

BASEDIR=../

for x in {1..128}; do
	rm -r $BASEDIR/input/$x
	rm -r $BASEDIR/runs/$x
	rm -r $BASEDIR/output/$x
done
