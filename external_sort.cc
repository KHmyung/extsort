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
#include <atomic>

#include "profile.h"
#include "debug.h"

#define TEST 1

#if TEST
#define INPUT_PATH "/mnt/test/input.txt"
#define OUTPUT_PATH "/mnt/test/output.txt"
#define RUNS_DIR_PATH "/mnt/test/runs/"
#define TOTAL_DATA_SIZE ((int64_t)128*1024*1024*1024) // (16GB)
#define MEM_SIZE ((int64_t)16*1024*1024*1024)	// (128MB)

#else
#define INPUT_PATH "./test/input.txt"
#define OUTPUT_PATH "./test/output.txt"
#define RUNS_DIR_PATH "./test/runs/"
#define TOTAL_DATA_SIZE ((int64_t)1*1024*1024*1024) // (16GB)
#define MEM_SIZE ((int64_t)128*1024*1024)	// (128MB)
#endif

#define DATA_SIZE (128)
#define NR_THREADS 1 // the number of buffer
#define BUFFER_SIZE (MEM_SIZE / NR_THREADS) // (run formation) mem buffer = run size
#define USE_EXISTING_DATA true
#define USE_EXISTING_RUNS false

#define NR_RUNS ((TOTAL_DATA_SIZE / MEM_SIZE) * NR_THREADS)	// (64 = 8 * 8)
#define BLK_SIZE ((MEM_SIZE / NR_RUNS) / 2)
#define BLK_PER_RUN (BUFFER_SIZE / BLK_SIZE)
#define NR_ENTRIES_BLK (BLK_SIZE / DATA_SIZE)
#define NR_ENTRIES_BUFFER (BUFFER_SIZE / DATA_SIZE)
#define NR_ENTRIES (TOTAL_DATA_SIZE / DATA_SIZE)
#define MRG_SIZE ((MEM_SIZE / 8) / 2)
#define NR_ENTRIES_MRG (MRG_SIZE / DATA_SIZE)

int64_t WriteData(int fd, char *buf, int64_t buf_size);
int64_t ReadData(int fd, char *buf, int64_t buf_size);
void* t_RunFormation(void *data);
void* t_Merge(void *data);

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


Data *g_buffer;
Data *blk_buffer;
Data *merge_buffer;

std::atomic_int run_idx;

struct RunformationArgs{
	int fd_input;
	int th_idx;
	off_t st_offset;
	char * r_buffer;
	int64_t nbyte_load;
};


bool compare(struct Data a, struct Data b){
	return  (a.key < b.key);
}

Data* alloc_buf(){
	void *mem;

	posix_memalign(&mem, 4096, MEM_SIZE);
	Data *tmp = new (mem) Data;

	return tmp;
}

void *randstring(size_t length, char *buf) {
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";
	if (length) {
		if (buf) {
			int l = (int) (sizeof(charset) -1);
			int key;
			for (int n = 0;n < length;n++) {
				key = rand() % l;
				buf[n] = charset[key];
			}
			buf[length] = '\0';
		}
	}
}


int GenerateDataFile(){
	srand((unsigned int)time(0));

	int fd = open( INPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
	//int fd = open( INPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | 0644);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file(%s) - %s\n", INPUT_PATH, strerror(errno));
	}

	DBG_P("file opend\n");

	int64_t nbyte_buffer = 0;
	int64_t nbyte_written = 0;
	int64_t offset = 0;

	for(size_t i=0; i<NR_ENTRIES; i++){

		randstring(sizeof(g_buffer[offset].value), g_buffer[offset].value);
		g_buffer[offset].key = rand() % UINT32_MAX;

		offset++;
		nbyte_buffer += DATA_SIZE;

		if(nbyte_buffer == MEM_SIZE){
			DBG_P("nbyte_buffer: %ld\n", nbyte_buffer);

			//char *buffer__ = (char *)g_buffer;

			int64_t tmp_nbyte_written = WriteData(fd, (char *)g_buffer, nbyte_buffer);
			if(tmp_nbyte_written == -1){
				fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
				return -1;
			}
			DBG_P("tmp_nbyte_written: %ld\n", tmp_nbyte_written);

			nbyte_written += tmp_nbyte_written;
			nbyte_buffer = 0;
			offset = 1;

			DBG_P("g_buffer flushed, nr_entries: %zu\n", i+1);
		}
	}

	if(nbyte_written != TOTAL_DATA_SIZE){
		fprintf(stderr, "FAIL: nbyte_written != TOTAL_DATA_SIZE\n");
		return -1;
	}

	close(fd);
}

int RunFormation(){

	run_idx = 0;

	pthread_t p_thread[NR_THREADS];
	int thread_id[NR_THREADS];
	struct RunformationArgs runformation_args[NR_THREADS];

	int fd_input = open( INPUT_PATH, O_RDONLY | O_DIRECT);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	off_t cumulative_offset = 0;
	for(int i=0; i<NR_THREADS; i++){
		runformation_args[i].fd_input = fd_input;
		runformation_args[i].th_idx = i;
		runformation_args[i].st_offset = cumulative_offset;
		runformation_args[i].r_buffer = (char *)g_buffer + cumulative_offset;
		runformation_args[i].nbyte_load = TOTAL_DATA_SIZE / NR_THREADS;


		thread_id[i] = pthread_create(&p_thread[i], NULL, t_RunFormation, (void*)&runformation_args[i]);

		cumulative_offset += BUFFER_SIZE;
	}

	int is_ok = 0;
	int res = 0;
	for(int i=0; i<NR_THREADS; i++){
		pthread_join(p_thread[i], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}

	close(fd_input);

	return res;
}

void* t_RunFormation(void *data){

	struct RunformationArgs args = *(struct RunformationArgs*)data;
	int fd_input = args.fd_input;
	char *r_buffer = args.r_buffer;
	Data *g_buffer__ = (Data *)r_buffer;
	int64_t nbyte_load = args.nbyte_load;

	int64_t nbyte_read = 0;
        struct timespec local_time[2];


	while(nbyte_read < nbyte_load){

		/* === PROFILE START (load)=== */
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
        int64_t tmp_byte_read = ReadData(fd_input, r_buffer, BUFFER_SIZE);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &storage_time[0], &storage_count[0]);
		/* === PROFILE END === */

        if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
			return ((void *)-1);
		}
		DBG_P("tmp_byte_read is: %zu\n", tmp_byte_read);

		nbyte_read += tmp_byte_read;
		DBG_P("nbyte_read: %ld\n", nbyte_read);


		std::sort(&g_buffer__[0], &g_buffer__[0] + NR_ENTRIES_BUFFER, compare);

		DBG_P("data sorted\n");

		std::string run_path = RUNS_DIR_PATH;
		run_path += "run_" + std::to_string(run_idx++);

		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
			return ((void *)-1);
		}

		/* === PROFILE START (flush) === */
        clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
        int64_t tmp_byte_write = WriteData(fd_run, (char *)g_buffer__, BUFFER_SIZE);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &storage_time[1], &storage_count[1]);
		/* === PROFILE END === */

        if(tmp_byte_write == -1){
			fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
			return ((void *)-1);
		}
		DBG_P("tmp_byte_write is: %zu\n", tmp_byte_read);


		DBG_P("run %d is done\n", run_idx-1);
		close(fd_run);
	}

	DBG_P("thread is done\n");
	return ((void *)1);
}

idx_Data Pop(std::priority_queue<idx_Data, std::vector<idx_Data>, std::greater<idx_Data>> *pq, off_t *blk_ofs, bool* blkbuf){

	idx_Data local_pop, local_push;
	local_pop = pq->top();
	pq->pop();

	local_push.idx = local_pop.idx;
	local_push.data = blk_buffer[(local_pop.idx * NR_ENTRIES_BLK * 2) + (blkbuf[local_pop.idx] * NR_ENTRIES_BLK) + blk_ofs[local_pop.idx]];

	pq->emplace(local_push);

	return local_pop;
}



void Merge(){

	bool idx = false;
	uint64_t sort_verify[2] = { 0, };
	int g_runcnt = NR_RUNS;
	int fd_runs[g_runcnt];
	off_t run_ofs[g_runcnt] = { 0, };	// BLK 단위
	off_t blk_ofs[g_runcnt] = { 0, };	// DATA 단위
	off_t mrg_ofs = 0;			// BLK 단위
	bool blkbuf[g_runcnt] = { 0, };
	bool mrgbuf = 0;

	merge_buffer = alloc_buf();
	blk_buffer = alloc_buf();

	std::priority_queue<idx_Data, std::vector<idx_Data>, std::greater<idx_Data>> pq;
        struct timespec local_time[2];

	for(int i = 0; i < g_runcnt; i++){
		std::string run_path = RUNS_DIR_PATH;
		run_path += "run_" + std::to_string(i);

		fd_runs[i] = open( run_path.c_str(), O_RDONLY | O_DIRECT);
		if(fd_runs[i] == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
		}

		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
		ReadData(fd_runs[i], (char*)(blk_buffer + (i * NR_ENTRIES_BLK * 2)), BLK_SIZE * 2);
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &storage_time[2], &storage_count[2]);
		run_ofs[i] += 2;

		idx_Data local_push;
		local_push.idx = i;
		local_push.data = blk_buffer[i * NR_ENTRIES_BLK * 2];
		blk_ofs[i]++;

		pq.push(local_push);
	}

	int64_t nbyte_merged = 0;
	int fd_output = open( OUTPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
	if(fd_output == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}

	while (nbyte_merged < TOTAL_DATA_SIZE){
		int run_idx = pq.top().idx;
		if(pq.top().data.key == 0){
			std::cout << "zero_" << run_idx << std::endl;
		}
		merge_buffer[(mrgbuf * NR_ENTRIES_MRG) + mrg_ofs] = pq.top().data;
		pq.pop();
		mrg_ofs++;

		if(blk_ofs[run_idx] > NR_ENTRIES_BLK){
			std::cout << "offset overflow" << std::endl;
		}

		if (mrg_ofs == NR_ENTRIES_MRG){
			sort_verify[idx] = merge_buffer[mrgbuf*NR_ENTRIES_MRG].key;
			if(sort_verify[idx] < sort_verify[!idx]){
				std::cout << "sort failed" << std::endl;
			}
			idx ^= true;

			clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
			WriteData(fd_output, (char *)(merge_buffer + (mrgbuf * NR_ENTRIES_MRG)), MRG_SIZE);
			clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
			calclock(local_time, &storage_time[3], &storage_count[3]);
			mrgbuf ^= true;
			nbyte_merged += MRG_SIZE;
			mrg_ofs = 0;
		}


		if((blk_ofs[run_idx] == NR_ENTRIES_BLK) && (run_ofs[run_idx] < BLK_PER_RUN)){
			clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
			ReadData(fd_runs[run_idx], (char*)(blk_buffer + (run_idx * NR_ENTRIES_BLK * 2) + (blkbuf[run_idx] * NR_ENTRIES_BLK)), BLK_SIZE);
			clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
			calclock(local_time, &storage_time[2], &storage_count[2]);
			blkbuf[run_idx] ^= true;
			blk_ofs[run_idx] = 0;
			run_ofs[run_idx]++;
		}
		if(blk_ofs[run_idx] == NR_ENTRIES_BLK) continue;

		idx_Data local_push;
		local_push.idx = run_idx;
		local_push.data = blk_buffer[(run_idx * NR_ENTRIES_BLK * 2) + (blkbuf[run_idx] * NR_ENTRIES_BLK) + blk_ofs[run_idx]];
		pq.push(local_push);
		blk_ofs[run_idx]++;
	}

	for(int i=0; i<g_runcnt; i++)
		close(fd_runs[i]);
	close(fd_output);
	free(merge_buffer);
	free(blk_buffer);
}


void PrintStat(){
	printf("RUN FORMATION\n");
	printf("%.2f;\n",(double)total_run_formation_time/1000000000);
	printf("MERGE;\n");
	printf("%.2f;\n",(double)total_merge_time/1000000000);

	std::cout << "RUNFORM" << storage_time[0]/1000000 << "/" << storage_time[1]/1000000 << std::endl;
	std::cout << "MERGE" << storage_time[2]/1000000 << "/" << storage_time[3]/1000000 << std::endl;

}

void PrintConfig(){
	printf("TOTAL_DATA_SIZE %zu\n", TOTAL_DATA_SIZE);
	printf("DATA_SIZE: %d\n", DATA_SIZE);
	printf("MEM_SIZE: %zu\n", MEM_SIZE);
	printf("NR_THREADS: %d\n", NR_THREADS);
	printf("NR_BUFFER: %d\n", NR_THREADS);
	printf("BUFFER_SIZE: %zu\n", BUFFER_SIZE);
	printf("NR_ENTRIES_BUFFER: %zu\n", NR_ENTRIES_BUFFER);
	printf("NR_ENTRIES: %zu\n", NR_ENTRIES);
}

int main(int argc, char* argv[]){

	int res = 0;

	g_buffer = alloc_buf();
	PrintConfig();
#if !USE_EXISTING_DATA
	if(GenerateDataFile() != -1){
		DBG_P("Data file generated\n");
	}
#endif

#if !USE_EXISTING_RUNS
	/* === PROFILE START (run formation)=== */
	struct timespec local_time1[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time1[0]);
	res = RunFormation();
	clock_gettime(CLOCK_MONOTONIC, &local_time1[1]);
	calclock(local_time1, &total_run_formation_time, &total_run_formation_count);
	/* === PROFILE END === */
	if(res != 0){
		fprintf(stderr, "FAIL: run formation\n");
		return -1;
	}
#endif


	/* merge */
	struct timespec local_time2[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time2[0]);
        Merge();        //      kh
	clock_gettime(CLOCK_MONOTONIC, &local_time2[1]);
	calclock(local_time2, &total_merge_time, &total_merge_count);

	PrintStat();
	return 0;
}
