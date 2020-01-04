#ifndef __DATAGEN_H
#define __DATAGEN_H

#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include "common.h"

struct DatagenArgs{
	int fd_input;
	int th_idx;
	int nr_thread;
	uint64_t data_size;
	uint64_t mem_size;
	uint64_t writeofs;
};

void DataGeneration(void*);
#endif
