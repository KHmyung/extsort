#ifndef __MERGE_H
#define __MERGE_H

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <algorithm>
#include <queue>
#include <stdlib.h>
#include "common.h"


struct MergeTime{
	unsigned long long merge_t;
	unsigned long long merge_c;
	unsigned long long *merge_arrival_t;
	unsigned long long *merge_arrival_c;
	unsigned long long *merge_read_t;
	unsigned long long *merge_read_c; 
	unsigned long long *merge_write_t; 
	unsigned long long *merge_write_c;
	unsigned long long *merge_sort_t;
	unsigned long long *merge_sort_c; 
};

struct MergeArgs{
	int th_id;
	int nr_run;
	int nr_range;
	int blk_size;
	int wrbuf_size;
	int *fd_run;
	uint64_t nr_entries;
	uint64_t *start_ofs;
	uint64_t *range_table;
	std::string outpath;
};

struct RangeInfo{
	int id;			/* range id */
	int mrg_ofs;		/* data entry offset of merge buffer */
	uint64_t merged;	/* number of entries merged*/
	Data *g_mrgbuf;		/* merge buffer for write */
	Data *g_blkbuf;		/* block buffer for merge thread */
};

struct RunInfo{
	int fd;			/* file descriptor of all run files */
	int run_ofs;		/* block offset of run file (from zero) */
	int last_blk;		/* number of blocks for corresponding range in each run */
	uint64_t read_ofs;	/* byte offset to be read in run file (from range offset) */
	uint64_t blk_ofs;	/* data entry to be pushed into priority queue */
	uint64_t blk_entry;	/* number of data entries in a block of each run */
	uint64_t remainder;	/* remaining data entries in the last block */
	Data *blkbuf;		/* block buffer for each run file of merge thread */
};

void Merge(void*);
extern struct MergeTime mrg_time;

#endif
