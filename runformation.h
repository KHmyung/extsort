#ifndef __RUNFORMATION_H
#define __RUNFORMATION_H

#include <atomic>
#include <algorithm>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include "common.h"

struct RunformationArgs{
	int fd_input;
	int th_id;
	int run_ofs;
	int nr_run;
	int nr_range;
	uint64_t data_size;
	uint64_t blk_size;
	uint64_t *range_table;
	std::string *runpath;
};

void RunFormation(void*);
extern struct TimeFormat run_time;

#endif /* __RUNFORMATION_H */

