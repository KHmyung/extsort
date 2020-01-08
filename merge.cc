#include "merge.h"

struct TimeFormat mrg_time;

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

static struct Data*
alloc_buf(uint64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
}

static int
refill_buffer(struct RunInfo *ri, int range){
	int ret = 0;
	int64_t size = ri->blk_entry * KV_SIZE;
	struct timespec local_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	}

	int64_t read_byte = ReadData(ri->fd, (char*)&ri->blkbuf[0], size);
	assert(read_byte >= 0);
	if(read_byte == 0){
		ri->done = 1;
		ret = 1;
	}
	ri->blk_entry = read_byte/KV_SIZE;
	ri->run_ofs++;
	ri->blk_ofs = 0;

	if(do_verify && ri->blk_entry > 1){
		for(int i = 0; i < ri->blk_entry - 1; i++){
			assert(ri->blkbuf[i].key <= ri->blkbuf[i+1].key);
		}
	}

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &mrg_time.read_t[range], &mrg_time.read_c[range]);
	}
	return ret;
}

static void
flush_buffer(int fd, char* buf, int64_t size, int id)
{
	struct timespec local_time[2];

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	}
	int64_t write_byte = WriteData(fd, &buf[0], size);
	assert(write_byte == size);
	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &mrg_time.write_t[id],
				&mrg_time.write_c[id]);
	}
}

static void
file_to_range(uint64_t *range_table, struct opt_t odb){
	FILE* meta = fopen(odb.metapath.c_str(), "r");

	fread(&range_table[0], sizeof(uint64_t), odb.nr_merge_th * odb.nr_run, meta);

	fclose(meta);
}

static void
init_range_info(struct RangeInfo *range_i, struct MergeArgs args){
	range_i->id = args.th_id;
	range_i->mrg_ofs = 0;
	range_i->merged = 0;
	range_i->g_blkbuf = alloc_buf(args.blk_size * args.nr_run);
	range_i->g_mrgbuf = alloc_buf(args.wrbuf_size);
}

static void
init_run_info(struct RunInfo *ri, Data* g_blkbuf, int fd, int run,
							uint64_t nr_entries, uint64_t blk_size)
{
	ri->fd = fd;
	ri->done = 0;
	ri->run_ofs = 0;
	ri->blk_ofs = 0;
	ri->blk_entry = blk_size/KV_SIZE;
	ri->remainder = nr_entries % ri->blk_entry;
	ri->last_blk = nr_entries / ri->blk_entry;

	/* buffer allocation for this run */
	ri->blkbuf = &g_blkbuf[run * (blk_size/KV_SIZE)];
}

static struct Data
init_tournament(struct RunInfo *ri, int range){
	struct Data first_data;

	refill_buffer(ri, range);
	first_data = ri->blkbuf[ri->blk_ofs++];

	return first_data;
}

static void
print_key(int id, int flag, uint64_t key){
	if(flag)
		std::cout << "min: " << key << "(range:" << id << ")" << std::endl;
	else
		std::cout << "max: " << key << "(range:" << id << ")" << std::endl;
}

static void
verify_state(struct RunInfo *ri, int range, uint64_t key)
{
	assert(ri->blk_ofs <= ri->blk_entry);
	assert(key != 0 || range == 0);
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
*t_Merge(void* data){
	struct MergeArgs args = *(struct MergeArgs*)data;

	id_Data slot;
	uint64_t blk_size = args.blk_size;
	int nr_run = args.nr_run;
	int nr_range = args.nr_range;
	uint64_t wrbuf_size = args.wrbuf_size;
	uint64_t nr_entries = args.nr_entries;
	uint64_t *range_table = args.range_table;
	uint64_t last_key;
	std::priority_queue<id_Data, std::vector<id_Data>, std::greater<id_Data>> pq;

	struct timespec thread_time[2];
	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
	}

	struct RangeInfo *range_i;
	range_i = (struct RangeInfo *)malloc(sizeof(struct RangeInfo));
	/* initialize range data structure */
	init_range_info(&range_i[0], args);

	struct RunInfo *run_i;
	run_i = (struct RunInfo *)malloc(nr_run * sizeof(struct RunInfo));

	for(int run = 0; run < nr_run; run++){
		/* initialize run data structure */
		init_run_info(&run_i[run], &range_i->g_blkbuf[0], args.fd_run[run],
												run, nr_entries, blk_size);

		/* initialize tournament */
		slot.data = init_tournament(&run_i[run], range_i->id);

		/* push the first item */
		slot.run_id = run;
		pq.push(slot);
	}


	/* open output file for this range */
	int fd_output = open( args.outpath.c_str(),
			O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE | O_TRUNC, 0644);

	if(do_verify){
		print_key(range_i->id, 1, pq.top().data.key);
	}

	/* start merging range */
	while(true){
		/* pick one from tournament */
		slot = pq.top();
		range_i->g_mrgbuf[range_i->mrg_ofs++] = slot.data;
		pq.pop();

		if(do_verify){
			last_key = slot.data.key;
			verify_state(&run_i[slot.run_id], range_i->id, slot.data.key);
		}

		/* if all entries are merged */
		if(++range_i->merged == nr_entries)
			break;

		/* flush full merge buffer */
		if(range_i->mrg_ofs == wrbuf_size/KV_SIZE){
			flush_buffer(fd_output, (char*)&range_i->g_mrgbuf[0],
							wrbuf_size, range_i->id);
			range_i->mrg_ofs = 0;
		}

		/* check block buffer is exhausted */
		if(run_i[slot.run_id].blk_ofs == run_i[slot.run_id].blk_entry){

			/* continue if the run is completed already */
			if(run_i[slot.run_id].done)
				continue;
			/* refill block */
			int ret = refill_buffer(&run_i[slot.run_id], range_i->id);
			if(ret) continue;
		}
		/* put one into tournament */
		slot.data = run_i[slot.run_id].blkbuf[run_i[slot.run_id].blk_ofs++];
		pq.push(slot);
	}


	if(do_verify){
		print_key(range_i->id, 0, last_key);
	}

	/* flush the last buffer */
	flush_buffer(fd_output, (char*)&range_i->g_mrgbuf[0], wrbuf_size, range_i->id);

	if(do_profile){
		clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
		calclock(thread_time, &mrg_time.arrival_t[range_i->id],
				&mrg_time.arrival_c[range_i->id]);
		mrg_time.sort_t[range_i->id]
			= mrg_time.arrival_t[range_i->id] -
			mrg_time.read_t[range_i->id] -
			mrg_time.write_t[range_i->id];
	}
	close(fd_output);

	free(range_i->g_blkbuf);
	free(range_i->g_mrgbuf);
	free(range_i);
	free(run_i);

	return (void*)1;
}

void
Merge(void* data){
	struct opt_t odb = *(struct opt_t *)data;
	int fd_run[odb.nr_merge_th][odb.nr_run];
	struct MergeArgs merge_args[odb.nr_merge_th];
	pthread_t mrg_thread[odb.nr_merge_th];
	for(int range = 0; range < odb.nr_merge_th; range++){
		for(int file = 0; file < odb.nr_run; file++){
			fd_run[range][file] =
				open( (odb.d_runpath[range] + std::to_string(file)).c_str(),
						O_DIRECT | O_RDONLY);
		}
	}

	time_init(odb.nr_merge_th, &mrg_time);
	uint64_t *range_table;
	range_table = (uint64_t *)calloc(odb.nr_merge_th * odb.nr_run, sizeof(uint64_t));

	file_to_range(&range_table[0], odb);

	uint64_t nr_entries;
	if(!odb.runform){
		nr_entries = print_range(&range_table[0], odb.nr_merge_th, odb.nr_run);
		assert(nr_entries == odb.total_size/KV_SIZE);
	}

	int range_ofs;
	for(int th = 0; th < odb.nr_merge_th; th++){
		nr_entries = 0;
		range_ofs = th * odb.nr_run;

		merge_args[th].th_id = th;
		merge_args[th].nr_run = odb.nr_run;
		merge_args[th].nr_range = odb.nr_merge_th;
		merge_args[th].blk_size = odb.mrg_blksize;
		merge_args[th].wrbuf_size = odb.mrg_wrbuf;
		merge_args[th].fd_run = &fd_run[th][0];
		merge_args[th].range_table = &range_table[range_ofs];

		for(int i = 0; i < odb.nr_run; i++){
			nr_entries += range_table[range_ofs + i];
		}
		merge_args[th].nr_entries = nr_entries;
		merge_args[th].outpath = odb.d_outpath[th] + std::to_string(th);

		pthread_create(&mrg_thread[th], NULL, t_Merge, &merge_args[th]);
	}

	int is_ok = 0;
	for(int th = 0; th < odb.nr_merge_th; th++){
		pthread_join(mrg_thread[th], (void **)&is_ok);
		assert(is_ok == 1);
	}

	free(range_table);
	for(int range = 0; range < odb.nr_merge_th; range++){
		for(int file = 0; file < odb.nr_run; file++){
			close(fd_run[range][file]);
		}
	}

}

