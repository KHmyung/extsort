#!/bin/bash

set -e

for x in {32,16,8,4,2}; do

	sudo ./sep_extsort.sh $x
	
#	sudo ./raid_extsort.sh $x

done
