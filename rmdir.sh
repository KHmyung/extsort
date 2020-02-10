#!/bin/bash

ulimit -n 65536

BASEDIR=/mnt/test/

sudo umount $BASEDIR/$i || /bin/true
