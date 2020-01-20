#!/bin/bash

# by Kihyeon Myung # Script for multi-drives test #

DEV_RAID="md0"
DEV="/dev/$DEV_RAID"


sudo umount $DEV || /bin/true
sudo mdadm --stop $DEV || /bin/true
sudo mdadm --remove $DEV || /bin/true

