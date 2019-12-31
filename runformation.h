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

struct RunformTime{
	unsigned long long runform_t;
	unsigned long long runform_c;
	unsigned long long *runform_arrival_t;
	unsigned long long *runform_arrival_c;
	unsigned long long *runform_read_t;
	unsigned long long *runform_read_c; 
	unsigned long long *runform_write_t; 
	unsigned long long *runform_write_c;
	unsigned long long *runform_sort_t;
	unsigned long long *runform_sort_c; 
};

struct RunformationArgs{
	int fd_input;
	int th_id;
	int run_ofs;
	int nr_run;
	int nr_range;
	uint64_t data_size;
	uint64_t blk_size;
	uint64_t offset;
	uint64_t *range_table;
	std::atomic<int> *run_id;
	std::string runpath;
};

void RunFormation(void*);
extern struct RunformTime run_time;

#endif /* __RUNFORMATION_H */

