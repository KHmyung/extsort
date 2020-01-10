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

struct MergeArgs{
	int th_id;
	int nr_run;
	int nr_range;
	int *fd_run;
	uint64_t blk_size;
	uint64_t wrbuf_size;
	uint64_t *blk_ofs;
	uint64_t *nr_entries;
	uint64_t *verify_key;
	uint64_t total_entries;
	std::string outpath;
};

struct RangeInfo{
	int id;			/* range id */
	int mrg_ofs;		/* data entry offset of merge buffer */
	uint64_t target;
	uint64_t merged;	/* number of entries merged*/
	Data *g_mrgbuf;		/* merge buffer for write */
	Data *g_blkbuf;		/* block buffer for merge thread */
};

struct RunInfo{
	int fd;			/* file descriptor of all run files */
	int run_ofs;		/* block offset of run file (from zero) */
	uint64_t blk_ofs;	/* data entry to be pushed into priority queue */
	uint64_t blk_entry;
	uint64_t nr_entries;
	uint64_t merged;
	Data *blkbuf;		/* block buffer for each run file of merge thread */
};

void Merge(void*);
extern struct TimeFormat mrg_time;

#endif
