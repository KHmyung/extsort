#include "runformation.h"

#define CURR 0
#define NEXT 1
#define ALIGN (4096/KV_SIZE)

struct TimeFormat run_time;

static unsigned long long * __time_alloc(int nr){
	return (unsigned long long *)calloc(nr, sizeof(unsigned long long));
}

static void time_init(int nr_thread, struct TimeFormat *time){
	time->total_t = 0;
	time->total_c = 0;
	time->arrival_t = __time_alloc(nr_thread);
	time->arrival_c = __time_alloc(nr_thread);
	time->read_t = __time_alloc(nr_thread);
	time->read_c = __time_alloc(nr_thread);
	time->write_t = __time_alloc(nr_thread);
	time->write_c = __time_alloc(nr_thread);
	time->sort_t = __time_alloc(nr_thread);
	time->sort_c = __time_alloc(nr_thread);
}


static struct Data* alloc_buf(uint64_t size){
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
		if(do_verify){
			std::cout << "range[" << range << "]: " << range_sum << std::endl;
		}
		nr_entries += range_sum;
	}
	return nr_entries;
}

static void
range_to_file(uint64_t *range_table, struct opt_t odb){
	int entries;

	/* switch rows to columns in range table for cache locality in merge phase */
	reverse_table(&range_table[0], odb.nr_run, odb.nr_merge_th);

	/* show range_run format */
	entries = print_range(&range_table[0], odb.nr_merge_th, odb.nr_run);
	assert(entries == odb.total_size/KV_SIZE);

	FILE* meta = fopen(odb.metapath.c_str(), "w+");

	/* put range table into file */
	fwrite(&range_table[0], sizeof(uint64_t), odb.nr_merge_th * odb.nr_run, meta);

	fclose(meta);
}

static void
refill_buffer(int fd, char *buf, int64_t size, int id){
	int64_t read_byte = 0;
	struct timespec local_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	}

	read_byte = ReadData(fd, &buf[0], size);
	assert(read_byte == size);

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &run_time.read_t[id], &run_time.read_c[id]);
	}
}

static uint64_t
flush_buffer(int fd, char *buf, int64_t size, int id){
	int64_t write_byte = 0;
	struct timespec local_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	}

	write_byte += WriteData(fd, &buf[0], size);
	assert(write_byte == size);

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &run_time.write_t[id], &run_time.write_c[id]);
	}
	return write_byte;
}

static int
range_record(uint64_t *table, uint64_t *ofs, uint64_t *size, int id){
	int ofs_align = 0;

	while(table[CURR] % ALIGN){
		table[CURR]++;
		ofs_align++;
	}
	size[CURR] = table[CURR];
	ofs[NEXT] = ofs[CURR] + table[CURR];

	return ofs_align;
}

static void
range_estimation(struct Data* buf, int nr_range, uint64_t size,
				int id, uint64_t *range_table, uint64_t *range_ofs,
				uint64_t *range_size)
{
	int range;
	int nr_filter = nr_range * nr_range;
	int filter = MAXKEY/nr_filter;

	uint64_t *buf_filter;
	buf_filter = (uint64_t*)calloc(nr_filter, sizeof(uint64_t));
	assert(buf_filter != NULL);

	uint64_t nr_entries = size/KV_SIZE;
	uint64_t boundary = (nr_range > 16) ? (nr_entries/nr_range)
										: (nr_entries/(nr_range + 1));

	for(int i = 0; i < nr_entries; i++){
		buf_filter[buf[i].key/filter]++;
	}

	range_ofs[0] = 0;
	range = 0;

	for(int j = 0; j < nr_filter; j++){
		range_table[range] += buf_filter[j];
		if((range_table[range] > boundary) && (range < nr_range - 1)){
			int align = range_record(&range_table[range], &range_ofs[range],
													&range_size[range], id);
			buf_filter[j+1] -= align;
			range++;
		}
	}
	range_size[range] = range_table[range];
	free(buf_filter);
}

static void
check_min(uint64_t *min, struct Data *runbuf, uint64_t *range_ofs, int nr_range)
{
	for(int range = 0; range < nr_range; range++){
		uint64_t this_min = runbuf[range_ofs[range]].key;
		if(this_min < min[range])
			min[range] = this_min;
	}
}

static void
check_max(uint64_t *max, struct Data *runbuf, uint64_t *range_ofs,
						uint64_t *range_size, int nr_range)
{
	for(int range = 0; range < nr_range; range++){
		uint64_t this_max = runbuf[range_ofs[range] + range_size[range] -1].key;
		if(this_max > max[range])
			max[range] = this_max;
	}
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
	uint64_t offset = 0;
	std::string *runpath = args.runpath;

	uint64_t *range_table = args.range_table;
	Data *runbuf = alloc_buf(blk_size);
	uint64_t range_ofs[nr_range];
	uint64_t range_size[nr_range];
	uint64_t min_key[nr_range];
	uint64_t max_key[nr_range] = { 0, };

	if(do_verify){
		for(int i = 0; i < nr_range; i++)
			min_key[i] = MAXKEY;
	}

	int done = 0;
	uint64_t nbyte_sorted = 0;

	struct timespec thread_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
	}

	while(done < nr_run){
		refill_buffer(fd_input, (char*)runbuf, blk_size, th_id);

		/* Sort (STL) */
		std::sort(&runbuf[0], &runbuf[blk_size/KV_SIZE], compare);

		/* gather range statistics from sorted buffer */
		range_estimation(&runbuf[0], nr_range, blk_size, th_id,
						&range_table[nr_range * done], &range_ofs[0], &range_size[0]);

		if(do_verify){
			check_min(&min_key[0], &runbuf[0], &range_ofs[0], nr_range);
			check_max(&max_key[0], &runbuf[0], &range_ofs[0], &range_size[0], nr_range);
		}

		for(int range = 0; range < nr_range; range++){
			int fd_run = open( (runpath[range] + std::to_string(run_ofs)).c_str(),
					O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0644);
			assert(fd_run > 0);
			int64_t write_byte = flush_buffer(fd_run,
					(char *)&runbuf[range_ofs[range]], range_size[range]*KV_SIZE, th_id);
			nbyte_sorted += write_byte;
			close(fd_run);
		}
		done++;
		run_ofs++;
	}

	assert(nbyte_sorted == data_size);

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
		calclock(thread_time, &run_time.arrival_t[th_id], &run_time.arrival_c[th_id]);
		run_time.sort_t[th_id] = run_time.arrival_t[th_id] -
							 	 run_time.read_t[th_id] -
								 run_time.write_t[th_id];
	}

	if(do_verify){
		for(int range = 0; range < nr_range; range++){
			std::cout << "[" << th_id << "] MIN_KEY: " << min_key[range] << std::endl;
			std::cout << "[" << th_id << "] MAX_KEY: " << max_key[range] << std::endl;
		}
	}
	free(runbuf);

	return ((void *)1);
}

void
RunFormation(void* data){
	struct opt_t odb = *(struct opt_t *)data;

	/* allocate range table */
	uint64_t *range_table;
	range_table = (uint64_t *)calloc(odb.nr_run, sizeof(uint64_t) * odb.nr_merge_th);
	assert(range_table != NULL);

	struct RunformationArgs runformation_args[odb.nr_runform_th];
	int thread_id[odb.nr_runform_th];
	pthread_t p_thread[odb.nr_runform_th];

	/* initialize timespec variable */
	time_init(odb.nr_runform_th, &run_time);

	int fd_input[odb.nr_runform_th];
	uint64_t data_size = odb.total_size/odb.nr_runform_th;
	int run_per_thread = odb.nr_run/odb.nr_runform_th;
	int run_ofs;
	for(int th = 0; th < odb.nr_runform_th; th++){
		fd_input[th] = open( odb.d_inpath[th].c_str(), O_DIRECT | O_RDONLY);
		assert(fd_input > 0);
		run_ofs = th * (odb.nr_run/odb.nr_runform_th);

		runformation_args[th].fd_input = fd_input[th];
		runformation_args[th].th_id = th;
		runformation_args[th].run_ofs = run_ofs;
		runformation_args[th].nr_run = run_per_thread;
		runformation_args[th].nr_range = odb.nr_merge_th;
		runformation_args[th].data_size = data_size;
		runformation_args[th].blk_size = odb.rf_blksize;
		runformation_args[th].runpath = &odb.d_runpath[0];
		runformation_args[th].range_table = &range_table[run_ofs * odb.nr_merge_th];

		thread_id[th] = pthread_create(&p_thread[th], NULL,
				t_RunFormation, (void*)&runformation_args[th]);
	}

	int is_ok = 0;
	for(int th = 0; th < odb.nr_runform_th; th++){
		pthread_join(p_thread[th], (void**)&is_ok);
		assert(is_ok == 1);
	}

	/* store range table into file */
	range_to_file(&range_table[0], odb);
	free(range_table);

	for(int th = 0 ; th < odb.nr_runform_th; th++){
		close(fd_input[th]);
	}
}


