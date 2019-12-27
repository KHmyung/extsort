#define _LARGEFILE63_SOURCE

#include <iostream>
#include <fstream>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <algorithm>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <queue>
#include <tbb/parallel_sort.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/concurrent_priority_queue.h>
#include <atomic>
#include <math.h>

#define INPUT_PATH "/mnt/sorting0/input.txt"
#define OUTPUT_PATH "/mnt/sorting0/output/"
#define RUN_PATH "/mnt/sorting0/runs/"
#define TOTAL_DATA_SIZE (((int64_t)1024*1024*1024*1024))
#define MEM_SIZE (((int64_t)128*1024*1024*1024))

#define DO_DATAGEN false
#define DO_TBB false
#define DO_RUNFORM true
#define DO_RANGE_MERGE true
#define DO_VERIFY false

#define SET_NUMA false
#define SET_AFFINITY false
#define NRTH_RUNFORM 2
#define NRTH_RANGEMRG 2
#define NRTH_TBB 4
#define NR_CORE 72

#define DATA_SIZE (128)
#define KEYRANGE 1073741824 // 0x40000000 1G(billion) key
#define NR_FILTER (NRTH_RANGEMRG*NRTH_RANGEMRG)
#define FILTER (KEYRANGE/NR_FILTER) 
#define NR_RANGE (NRTH_RANGEMRG)

#define BUFFER_SIZE (MEM_SIZE / NRTH_RUNFORM)
#define NR_RUNS ((TOTAL_DATA_SIZE / MEM_SIZE) * NRTH_RUNFORM)	// 1024
#define NR_ENTRIES_BUFFER (BUFFER_SIZE / DATA_SIZE)
#define NR_ENTRIES (TOTAL_DATA_SIZE / DATA_SIZE)
#define MRG_SIZE (1*1024*1024)
#define NR_ENTRIES_MRG (MRG_SIZE / DATA_SIZE)

#define DO_PROFILE true
#define NRTH_DATAGEN 64

int queue_mask[NR_CORE]={1,4,7,9,11,13,15,17,19,22,25,27,29,31,33,35,37,40,43,45,47,49,51,53,55,58,61,63,65,67,69,71,2,5,8,10,12,14,16,18,20,23,26,28,30,32,34,36,38,41,44,46,48,50,52,54,56,59,62,64,66,68,70,72,3,6,21,24,39,42,57,60};

struct Data{
	uint64_t key;
	char value[DATA_SIZE-sizeof(uint64_t)];
};

struct idx_Data{
	uint32_t idx;
	struct Data data;
	bool operator>(const idx_Data &cmp) const {
		return data.key > cmp.data.key;
	}
};

struct DatagenArgs{
	int fd_input;
	int th_idx;
	uint64_t writeofs;
};

struct RunformationArgs{
	int fd_input;
	int th_idx;
	off_t st_offset;
	char * r_buffer;
	int64_t nbyte_load;
};

struct GroupMergeArgs{
	int th_idx;
	std::string inpath;
};

struct RangeMergeArgs{
	int th_idx;
	int fd_output;
};

Data *g_buffer;
int run_idx;

int fd_mrgs[NR_RUNS];
uint64_t g_keydist[NRTH_RANGEMRG];
uint64_t g_ofs_range[NRTH_RANGEMRG];
uint64_t g_ofs_file[NR_RUNS];
uint64_t keydist[NR_RUNS][NRTH_RANGEMRG];


extern bool compare(struct Data a, struct Data b){
	return  (a.key < b.key);
}

extern Data* alloc_buf(){
	void *mem;

	posix_memalign(&mem, 4096, MEM_SIZE);
	Data *tmp = new (mem) Data;

	return tmp;
}

extern void *randstring(size_t length, char *buf, unsigned int seed) {
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";
	if (buf) {
		int l = (int) (sizeof(charset) -1);
		int key;
		for (int n = 0;n < length;n++) {        
			key = rand_r(&seed) % l;
			buf[n] = charset[key];
		}
		buf[length] = '\0';
	}
}

extern struct Data* range_balancing(struct Data* buf, size_t size, int run){
	uint64_t dist[NR_FILTER] = {0,};
	int range = 0;
	
	for(int i=0; i<size; i++){
		dist[(buf[i].key)/FILTER]++;
	}

	if( NRTH_RANGEMRG > 16){
		for(int j=0; j<NR_FILTER; j++){
			keydist[run][range] += dist[j];
			if((keydist[run][range] > size/(NR_RANGE)) && (range < NR_RANGE-1)){
				range++;
			}
		}
	}
	else {
		for(int j=0; j<NR_FILTER; j++){
			keydist[run][range] += dist[j];
			if((keydist[run][range] > size/(NR_RANGE+1)) && (range < NR_RANGE-1)){
				range++;
			}
		}
	}
	
}


int64_t WriteData(int fd, char *buf, int64_t buf_size);
int64_t ReadData(int fd, char *buf, int64_t buf_size);
int64_t pWriteData(int fd, char *buf, int64_t buf_size, off_t ofs);
int64_t pReadData(int fd, char *buf, int64_t buf_size, off_t ofs);
void my_sort(struct Data* base, int64_t left, int64_t right, uint64_t *dist, int range);
void* t_GenerateDataFile(void *data);
void* t_RunFormation(void *data);
void* t_GroupMerge(void *data);
void* t_RangeMerge(void *data); 
