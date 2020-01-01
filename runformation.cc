#include "runformation.h"

struct RunformTime run_time;

static unsigned long long * __time_alloc(int nr){
	return (unsigned long long *)calloc(nr, sizeof(unsigned long long));
}

static void time_init(int nr_thread, struct RunformTime *time){
	time->runform_t = 0;
	time->runform_c = 0;
	time->runform_arrival_t = __time_alloc(nr_thread);
	time->runform_arrival_c = __time_alloc(nr_thread);
	time->runform_read_t = __time_alloc(nr_thread);
	time->runform_read_c = __time_alloc(nr_thread);
	time->runform_write_t = __time_alloc(nr_thread);
	time->runform_write_c = __time_alloc(nr_thread);
	time->runform_sort_t = __time_alloc(nr_thread);
	time->runform_sort_c = __time_alloc(nr_thread);
}


static struct Data* alloc_buf(int64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
}

static bool 
compare(struct Data a, struct Data b){
	return  (a.key < b.key);
}

static void 
range_balancing(struct Data* buf, int nr_range, uint64_t size, 
				int run, uint64_t *range_table, int id)
{
	int range = 0;
	int nr_filter = nr_range * nr_range;
	int filter = MAXKEY/nr_filter;

	uint64_t *buf_filter;
	buf_filter = (uint64_t*)calloc(nr_filter, sizeof(uint64_t));
	assert(buf_filter != NULL);

	uint64_t nr_entries = size/KV_SIZE;

	for(int i = 0; i < nr_entries; i++){
		buf_filter[buf[i].key/filter]++;
	}

	memset(&range_table[0], 0, nr_range);

	if(nr_range > 16){
		for(int j = 0; j < nr_filter; j++){
			range_table[range] += buf_filter[j];
			if((range_table[range] > nr_entries/nr_range) && 
					(range < nr_range-1))
			{
				range++;
			}
		}
	}
	else {
		for(int j = 0; j < nr_filter; j++){
			range_table[range] += buf_filter[j];
			if((range_table[range] > nr_entries/(nr_range + 1)) && 
					(range < nr_range-1))
			{
				range++;
			}
		}
	}
	free(buf_filter);
}

static void
reverse_table(uint64_t *table, int nr_run, int nr_range){
	uint64_t *new_table;
	new_table = (uint64_t *)malloc(nr_run * nr_range * sizeof(uint64_t));
	assert(new_table != NULL);

	for(int run = 0; run < nr_run; run++){
		/* data moved from row to column */
		for(int range = 0; range < nr_range; range++){
			new_table[range * nr_run + run] = table[run * nr_range + range];
		}
	}

	memcpy(table, new_table, nr_run * nr_range * sizeof(uint64_t));	
	free(new_table);
}


static uint64_t
print_range(uint64_t *range_table, int nr_range, int nr_run){
	uint64_t range_sum;
	uint64_t nr_entries = 0;
	for(int range = 0; range < nr_range; range++){
		range_sum = 0;
		for(int run = 0; run < nr_run; run++){
			range_sum += range_table[range * nr_run + run];
		}
		std::cout << "range[" << range << "]: " << range_sum << std::endl;
		nr_entries += range_sum;
	}
	return nr_entries;	
}

static void
calc_start_ofs(uint64_t *range_table, uint64_t *start_ofs, 
					int boundary, int nr_range, int nr_run)
{
	uint64_t ofs;
	
	for(int run = 0; run < nr_run; run++){
		ofs = 0;
		for(int range = 0; range < boundary; range++){
			ofs += range_table[run * nr_range + range];
		
		}	
		start_ofs[run] = ofs;
	}
}

static void 
range_to_file(uint64_t *range_table, struct opt_t odb){
	int entries;
	uint64_t *start_ofs;
	start_ofs = (uint64_t *)malloc(odb.nr_merge_th * odb.nr_run * sizeof(uint64_t));

	/* range boundary calculation for each run file */
	for (int range = 0; range < odb.nr_merge_th; range++){
		calc_start_ofs(&range_table[0], &start_ofs[range * odb.nr_run], range,
					odb.nr_merge_th, odb.nr_run);
	}

	/* switch rows to columns in range table for cache locality in merge phase */
	reverse_table(&range_table[0], odb.nr_run, odb.nr_merge_th);
		
	/* show range_run format */
	entries = print_range(&range_table[0], odb.nr_merge_th, odb.nr_run);
	assert(entries == odb.total_size/KV_SIZE);

	FILE* meta = fopen(odb.metapath.c_str(), "w+");
	
	/* put range table into file */	
	fwrite(&range_table[0], sizeof(uint64_t), odb.nr_merge_th * odb.nr_run, meta);
	fwrite(&start_ofs[0], sizeof(uint64_t), odb.nr_merge_th * odb.nr_run, meta);
	
	free(start_ofs);
	fclose(meta);
}

static void 
refill_buffer(int fd, char *buf, uint64_t size, int id){
	uint64_t read_byte;
	struct timespec local_time[2];
	
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif

	read_byte = read(fd, buf, size);
	assert(read_byte == size);

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &run_time.runform_read_t[id], &run_time.runform_read_c[id]);
#endif
}

static uint64_t 
flush_buffer(int fd, char *buf, uint64_t size, int id){
	uint64_t write_byte;
	struct timespec local_time[2];
	
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif

	write_byte = write(fd, buf, size);
	assert(write_byte == size);

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &run_time.runform_write_t[id], &run_time.runform_write_c[id]);
#endif
	return write_byte;
}

static void * 
t_RunFormation(void *data){
	struct RunformationArgs args = *(struct RunformationArgs*)data;
	int fd_input = args.fd_input;
	int run_ofs = args.run_ofs;
	int nr_run = args.nr_run;
	int nr_range = args.nr_range;
	int th_id = args.th_id;
	uint64_t data_size = args.data_size;
	uint64_t blk_size = args.blk_size;
	uint64_t offset = args.offset;
	std::string runpath = args.runpath;
	
	uint64_t *range_table = args.range_table;
	Data *runbuf = alloc_buf(blk_size);

	int done = 0;
	uint64_t nbyte_sorted = 0;
	
	struct timespec thread_time[2];

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
#endif

	while(done < nr_run){
		refill_buffer(fd_input, (char*)runbuf, blk_size, th_id);

		/* Sort (STL) */	
		std::sort(&runbuf[0], &runbuf[blk_size/KV_SIZE], compare);

		/* gather range statistics from sorted buffer */		
		range_balancing(&runbuf[0], nr_range, blk_size, run_ofs, 
				&range_table[nr_range * done], th_id);

		int fd_run = open( (runpath + std::to_string(run_ofs)).c_str(),
			       	O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		assert(fd_run != -1);
	
	        int64_t write_byte = flush_buffer(fd_run, (char *)&runbuf[0], blk_size, th_id);
		nbyte_sorted += write_byte;		

		done++;
		run_ofs++;
		
		close(fd_run);
	}

	assert(nbyte_sorted == data_size); 

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
	calclock(thread_time, &run_time.runform_arrival_t[th_id], &run_time.runform_arrival_c[th_id]);
	run_time.runform_sort_t[th_id] = run_time.runform_arrival_t[th_id] - run_time.runform_read_t[th_id] -
							run_time.runform_write_t[th_id];
#endif
	free(runbuf);

	return ((void *)1);
}

void 
RunFormation(void* data){
	struct opt_t odb = *(struct opt_t *)data;
	
	std::atomic<int> run_id = { 0 };

	/* allocate range table */
	uint64_t *range_table;
	range_table = (uint64_t *)calloc(odb.nr_run, sizeof(uint64_t) * odb.nr_merge_th);
	assert(range_table != NULL);

	struct RunformationArgs runformation_args[odb.nr_runform_th];
	int thread_id[odb.nr_runform_th];
	pthread_t p_thread[odb.nr_runform_th];

	/* initialize timespec variable */
	time_init(odb.nr_runform_th, &run_time);	

	int fd_input = open( odb.inpath.c_str(), O_DIRECT | O_RDONLY);
	assert(fd_input != -1);
	
	int data_size = odb.total_size/odb.nr_runform_th;
	int run_per_thread = odb.nr_run/odb.nr_runform_th;
	int run_ofs;
	for(int th_id = 0; th_id < odb.nr_runform_th; th_id++){
		run_ofs = th_id * (odb.nr_run/odb.nr_runform_th);

		runformation_args[th_id].fd_input = fd_input;
		runformation_args[th_id].th_id = th_id;
		runformation_args[th_id].run_ofs = run_ofs;
		runformation_args[th_id].nr_run = run_per_thread;
		runformation_args[th_id].nr_range = odb.nr_merge_th;
		runformation_args[th_id].data_size = data_size;
		runformation_args[th_id].blk_size = odb.rf_blksize;
		runformation_args[th_id].offset = data_size * run_ofs;
		runformation_args[th_id].run_id = &run_id;
		runformation_args[th_id].runpath = odb.runpath;
		runformation_args[th_id].range_table = &range_table[run_ofs * odb.nr_merge_th];

		thread_id[th_id] = pthread_create(&p_thread[th_id], NULL, 
				t_RunFormation, (void*)&runformation_args[th_id]);
	}
	
	uint64_t is_ok = 0;
	for(int th_id = 0; th_id < odb.nr_runform_th; th_id++){
		pthread_join(p_thread[th_id], (void**)&is_ok);
	}

	/* store range table into file */	
	range_to_file(&range_table[0], odb);

	close(fd_input);
	free(range_table);
}


