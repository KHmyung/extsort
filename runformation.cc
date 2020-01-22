#include "runformation.h"
#include <fcntl.h>
#define CURR 0
#define NEXT 1
#define DIO_ALIGN (4096)
#define ENTRY_ALIGN (4096/KV_SIZE)

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
	new_table = (uint64_t *)malloc(2 * nr_run * nr_range * sizeof(uint64_t));
	assert(new_table != NULL);

	for(int run = 0; run < nr_run; run++){
		/* data moved from row to column */
		for(int range = 0; range < nr_range; range++){
			new_table[range * nr_run + run] = table[run * nr_range + range];
			new_table[range * nr_run + run + 1] = table[run * nr_range + range + 1];
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
store_meta(struct RunDesc *rd, int nr_array, std::string path){

	FILE* meta = fopen(path.c_str(), "w+");

	/* put range table into file */
	fwrite(&rd[0], sizeof(struct RunDesc), nr_array, meta);

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

static void
random_sampling(std::string *filename, uint64_t filesize, int nr_file,
				struct Data *buf, uint64_t sample_count)
{
	srand((unsigned int)time(0));

	uint64_t done = 0;
	int fd[nr_file];
	for(int file = 0; file < nr_file; file++){
		fd[file] = open( filename[file].c_str(), O_DIRECT | O_RDONLY);
		assert(fd[file] > 0);
	}
	while(done < sample_count){
		int file = rand() % nr_file;
		uint64_t rand_ptr = rand() % (filesize/nr_file/DIO_ALIGN);
		pread(fd[file], &buf[0] + done*ENTRY_ALIGN, DIO_ALIGN, rand_ptr*DIO_ALIGN);

		done++;
	}

	for(int file = 0; file < nr_file; file++)
		close(fd[file]);
}

static void
range_estimation(struct Data* buf, uint64_t *p, int nr_range, uint64_t size)
{
	uint64_t nr_entries = size/KV_SIZE;
	uint64_t boundary_ofs = nr_entries/nr_range;

	std::sort(&buf[0], buf + nr_entries, compare);

	p[0] = 0;

	for(int range = 1; range < nr_range; range++){
		p[range] = buf[boundary_ofs * range].key;
	}

	if(do_verify){
		std::cout << "\nRange Estimation (key)" << std::endl;
		for(int range = 0; range < nr_range; range++){
			std::cout << "[Range" << range << "] : " << p[range] << std::endl;
		}
	}
}

static void
range_partitioning(struct Data *buf, uint64_t *p, uint64_t size,
			int nr_range, struct RunDesc *rd, int id)
{
	int range = 0;
	uint64_t cnt = 0;
	uint64_t sum = 0;
	uint64_t len = size/KV_SIZE;

	for(uint64_t ofs = 0; ofs < len; ofs++){

		if(buf[ofs].key > p[range + 1]){
			rd[range].valid_entries = cnt;
			sum += cnt;
			cnt = 0;

			range++;
		}

		if(range == nr_range - 1){
			rd[range].valid_entries = len - ofs;
			sum += rd[range].valid_entries;
			break;
		}

		cnt++;
	}
	assert(sum == len);

}

static void
range_calibration(struct RunDesc *rd, uint64_t mrg_blk_size, int nr_range, int id)
{
	uint64_t cum_ofs, start, end;

	for(int range = 0; range < nr_range; range++){
		cum_ofs = 0;
		for(int j = 0; j < range; j++){
			cum_ofs += rd[j].valid_entries;
		}

		start = cum_ofs * KV_SIZE;
		end = (cum_ofs + rd[range].valid_entries) * KV_SIZE;

		/* first data offset in the first block */
		rd[range].blk_ofs = (start % DIO_ALIGN) / KV_SIZE;

		/* write offset */
		rd[range].rw_ofs = (start / DIO_ALIGN) * DIO_ALIGN / KV_SIZE;

		/* write size */
		rd[range].rw_size = (((end / DIO_ALIGN) - (start / DIO_ALIGN)) * DIO_ALIGN);
		if(end % DIO_ALIGN){
			rd[range].rw_size += DIO_ALIGN;
		}
	}
}

static void
record_run_id(struct RunDesc *rd, int nr_range, int run){

	for(int range = 0; range < nr_range; range++){

		rd[range].run_id = run;
	}
}

static void *
t_RunFormation(void *data)
{
	struct RunformationArgs args = *(struct RunformationArgs*)data;
	int fd_input = args.fd_input;
	int run_ofs = args.run_ofs;
	int nr_run = args.nr_run;
	int nr_range = args.nr_range;
	int th_id = args.th_id;
	uint64_t data_size = args.data_size;
	uint64_t blk_size = args.blk_size;
	uint64_t mrg_blk_size = args.mrg_blk_size;
	uint64_t offset = 0;
	std::string *runpath = args.runpath;
	uint64_t *partition = args.partition;
	struct RunDesc *run_d = args.run_d;

	Data *runbuf = alloc_buf(blk_size);
	uint64_t write_ofs[nr_range];
	uint64_t write_size[nr_range] = {0, };

	int done = 0;
	uint64_t sorted_entries = 0;

	struct timespec thread_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
	}

	while(done < nr_run){

		record_run_id(&run_d[done * nr_range], nr_range, run_ofs);

		refill_buffer(fd_input, (char*)runbuf, blk_size, th_id);

		/* Sort (STL) */
		std::sort(&runbuf[0], runbuf + blk_size/KV_SIZE, compare);

		range_partitioning(&runbuf[0], &partition[0], blk_size, nr_range,
						   &run_d[done * nr_range], th_id);

		range_calibration(&run_d[done * nr_range], mrg_blk_size, nr_range, th_id);

		/* Shuffle */
		for(int range = 0; range < nr_range; range++){

			int fd_run = open( (runpath[range] + std::to_string(run_ofs)).c_str(),
					O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0644);
			assert(fd_run > 0);

			int64_t write_byte =
			  flush_buffer(fd_run, (char*)&runbuf[run_d[done * nr_range + range].rw_ofs],
							run_d[done * nr_range + range].rw_size, th_id);
			sorted_entries += run_d[done * nr_range + range].valid_entries;
			close(fd_run);
		}

		done++;
		run_ofs++;
	}

	assert(sorted_entries == data_size/KV_SIZE);

	if(do_verify){
		std::cout << "Thread completed (" << th_id << ")" << std::endl;
	}
	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
		calclock(thread_time, &run_time.arrival_t[th_id], &run_time.arrival_c[th_id]);
		run_time.sort_t[th_id] = run_time.arrival_t[th_id] -
							 	 run_time.read_t[th_id] -
								 run_time.write_t[th_id];
	}
	free(runbuf);

	return ((void *)1);
}



void
RunFormation(void* data){


	struct opt_t odb = *(struct opt_t *)data;

	uint64_t *partition;
	partition = (uint64_t *)malloc(odb.nr_merge_th * sizeof(uint64_t));

	struct RunDesc *run_d;
	run_d = (struct RunDesc *)malloc(odb.nr_run * odb.nr_merge_th *
						sizeof(struct RunDesc));

	struct RunformationArgs runformation_args[odb.nr_runform_th];
	int thread_id[odb.nr_runform_th];
	pthread_t p_thread[odb.nr_runform_th];

	/* initialize timespec variable */
	time_init(odb.nr_runform_th, &run_time);

	uint64_t data_size = odb.total_size/odb.nr_runform_th;
	int run_per_thread = odb.nr_run/odb.nr_runform_th;
	int nr_file = odb.nr_runform_th;

	uint64_t sample_size = odb.total_size/1024;
	Data* randbuf = alloc_buf(sample_size);

	/* gather random samples */
	random_sampling(&odb.d_inpath[0], odb.total_size, nr_file,
					&randbuf[0], sample_size/DIO_ALIGN);

	/* set partition boundary through random samples */
	range_estimation(&randbuf[0], &partition[0], odb.nr_merge_th, sample_size);

	int fd_input[nr_file];
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
		runformation_args[th].mrg_blk_size = odb.mrg_blksize;
		runformation_args[th].runpath = &odb.d_runpath[0];
		runformation_args[th].partition = &partition[0];
		runformation_args[th].run_d = &run_d[run_ofs * odb.nr_merge_th];

		thread_id[th] = pthread_create(&p_thread[th], NULL,
				t_RunFormation, (void*)&runformation_args[th]);
	}

	int is_ok = 0;
	for(int th = 0; th < odb.nr_runform_th; th++){
		pthread_join(p_thread[th], (void**)&is_ok);
		assert(is_ok == 1);
	}

	/* store runfile information into file */
	store_meta(&run_d[0], odb.nr_run * odb.nr_merge_th, odb.metapath);
	free(partition);
	free(run_d);

	for(int th = 0 ; th < odb.nr_runform_th; th++){
		close(fd_input[th]);
	}
}


