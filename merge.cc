#include "merge.h"

struct MergeTime mrg_time;
std::priority_queue<id_Data, std::vector<id_Data>, std::greater<id_Data>> pq;

static struct Data* 
alloc_buf(int64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
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
	range_i->nbyte_merged = 0;
	range_i->g_blkbuf = alloc_buf(args.blk_size * args.nr_run);
	range_i->g_mrgbuf = alloc_buf(args.wrbuf_size);
}

static void
init_run_info(struct RunInfo *run_i, Data* g_blkbuf, uint64_t nr_entries, int fd, int run, int id, 
				int nr_range, int blk_size, uint64_t start_ofs) 
{
	uint64_t end_ofs;
	run_i->fd = fd; 
	run_i->run_ofs = 0;
	start_ofs *= KV_SIZE;
	run_i->read_ofs = start_ofs / blk_size;
	run_i->read_ofs *= blk_size;

	run_i->blk_ofs = (start_ofs % blk_size) / KV_SIZE;
	run_i->blk_entry = blk_size / KV_SIZE;

	end_ofs = start_ofs + nr_entries * KV_SIZE;
	run_i->nr_blk = (end_ofs / blk_size) - (run_i->read_ofs / blk_size) + 1;
	run_i->remainder = (end_ofs % blk_size) / KV_SIZE;
	run_i->blkbuf = g_blkbuf + (run * blk_size / KV_SIZE);

	if(run_i->remainder == 0) 
		run_i->nr_blk--;
}

static struct Data
init_tournament(int fd, 
		int id,				/* range id */
		struct Data *blkbuf, 
		int *run_ofs,
		uint64_t *read_ofs, 
		uint64_t *blk_ofs,
		int blk_size)
{
	struct Data first_data;
	struct timespec local_time[2];

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif		

	int64_t tmp_read = pread(fd, (char*)&blkbuf[0], blk_size, *read_ofs);
	assert(tmp_read == blk_size);

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &mrg_time.merge_read_t[id], &mrg_time.merge_read_c[id]);
#endif
	*read_ofs += tmp_read;	
	*run_ofs++;

	first_data = blkbuf[0];
	*blk_ofs++;

	return first_data;
}

static void
pull_one(struct id_Data *slot, struct Data *mrgbuf, int *mrg_ofs)
{
	*slot = pq.top();
	
	mrgbuf[*mrg_ofs] = slot->data;
	(*mrg_ofs)++;

	pq.pop();
}

static void
push_one(struct id_Data *slot, struct Data *blkbuf, uint64_t *blk_ofs, int blk_size)
{
	slot->data = blkbuf[*blk_ofs];
	*blk_ofs++;
	pq.push(*slot);
}


static void
print_key(int id, int flag, uint64_t key){
	if(flag)
		std::cout << "min: " << key << "(range:" << id << ")" << std::endl;
	else
		std::cout << "max: " << key << "(range:" << id << ")" << std::endl;
}

static void
verify_state(struct RangeInfo *range_i, struct RunInfo *run_i, struct id_Data slot)
{
	assert(run_i[slot.run_id].blk_ofs <= run_i[slot.run_id].blk_entry);
	assert(!(slot.data.key == 0 && range_i->id != 0));
}

static uint64_t
refill_buffer(int fd, char* buf, int size, int id, uint64_t ofs){
#if DO_PROFILE
	struct timespec local_time[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
	int64_t tmp_read = pread(fd, (char*)buf, size, ofs);
	assert(tmp_read == size);
	ofs += tmp_read;
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &mrg_time.merge_read_t[id], 
			     &mrg_time.merge_read_c[id]);
#endif
	return ofs;
}


static void
flush_buffer(int fd, char* buf, int size, int id)
{
#if DO_PROFILE
	struct timespec local_time[2];
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
	int64_t tmp_write = write(fd, (char*)buf, size);
	assert(tmp_write == size);
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &mrg_time.merge_write_t[id], 
			     &mrg_time.merge_write_c[id]);
#endif
}

static void 
*t_Merge(void* data){
	struct MergeArgs args = *(struct MergeArgs*)data;
	
	id_Data slot;
	int blk_size = args.blk_size;
	int nr_run = args.nr_run;
	int nr_range = args.nr_range;
	int wrbuf_size = args.wrbuf_size;
	int nr_entries = args.nr_entries;
	uint64_t *range_table = args.range_table;
	struct timespec thread_time[2];
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
#endif

	struct RangeInfo *range_i;
	range_i = (struct RangeInfo *)malloc(sizeof(struct RangeInfo));
	/* initialize range data structure */
	init_range_info(&range_i[0], args);
	
	struct RunInfo *run_i;
	run_i = (struct RunInfo *)malloc(nr_run * sizeof(struct RunInfo));
	
	uint64_t start_ofs;
	for(int run = 0; run < nr_run; run++){
		start_ofs = 0;
		/* set start offset of each run file */
		for(int range = 0; range < range_i->id; range++)
			start_ofs += range_table[run * nr_range + range];
		/* initialize run data structure */
		init_run_info(&run_i[run], &range_i->g_blkbuf[0], nr_entries, 
				args.fd_run[run], run, range_i->id, 
				nr_range, blk_size, start_ofs);
		/* initialize tournament */
		slot.data = init_tournament(run_i[run].fd, 
					    range_i->id,
					    &run_i[run].blkbuf[0],
					    &run_i[run].run_ofs,
					    &run_i[run].read_ofs,
					    &run_i[run].blk_ofs,
					    blk_size);
		slot.run_id = run;
		/* push the first item */
		pq.push(slot);
	}
	
	/* open output file for this range */
	int fd_output = open( args.outpath.c_str(),
		       	O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);

#if DO_VERIFY
	print_key(range_i.id, 1, pq.top().data.key);
#endif

	/* start merging range */
	while(true){
		/* pull one item from tournament */
		pull_one(&slot, range_i->g_mrgbuf, &range_i->mrg_ofs);
		range_i->nbyte_merged++;

		/* if all entries are merged */
		if(range_i->nbyte_merged == nr_entries)
			break;
#if DO_VERIFY
		verify_state(&range_i, &run_i, slot);
#endif

		/* flush merge buffer if full */
		if(range_i->mrg_ofs == wrbuf_size/KV_SIZE){
			flush_buffer(fd_output, (char*)&range_i->g_mrgbuf[0], 
					wrbuf_size,
					range_i->id);
			range_i->mrg_ofs = 0;
		}

		/* refill block buffer if empty */
		if(run_i[slot.run_id].blk_ofs == run_i[slot.run_id].blk_entry){
			/* continue if this run is exhausted */ 
			if(run_i[slot.run_id].run_ofs == run_i[slot.run_id].nr_blk)
				continue;
			
			/* refill block */
			else{
				run_i[slot.run_id].read_ofs =
					refill_buffer(run_i[slot.run_id].fd, 
					      (char*)&run_i[slot.run_id].blkbuf[0],
					      blk_size, range_i->id,
					      run_i[slot.run_id].read_ofs);
				run_i[slot.run_id].blk_ofs = 0;
				run_i[slot.run_id].run_ofs++;
			}
			
			/* check whether this is the last block */
			if(run_i[slot.run_id].run_ofs == run_i[slot.run_id].nr_blk - 1 &&
					run_i[slot.run_id].remainder != 0){
				run_i[slot.run_id].blk_entry = run_i[slot.run_id].remainder;
				memset(&run_i[slot.run_id].blkbuf[0], 0, blk_size);
			}
		}
		/* push one item into tournament */ 
		push_one(&slot, &run_i[slot.run_id].blkbuf[0], 
				&run_i[slot.run_id].blk_ofs, blk_size);
	}				


#if DO_VERIFY	
	print_key(range_i.id, 0, range_i.g_mrgbuf[0].key);
#endif
	
	/* flush last buffer */
	flush_buffer(fd_output, (char*)&range_i->g_mrgbuf[0], wrbuf_size, range_i->id);

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
	calclock(thread_time, &mrg_time.merge_arrival_t[range_i->id],
		              &mrg_time.merge_arrival_c[range_i->id]);
	mrg_time.merge_sort_t[range_i->id] 
		= mrg_time.merge_arrival_t[range_i->id] -
		  mrg_time.merge_read_t[range_i->id] -
		  mrg_time.merge_write_t[range_i->id];		
#endif
	close(fd_output);

	free(range_i->g_blkbuf);
	free(range_i->g_mrgbuf);
}

void 
Merge(void* data){
	struct opt_t odb = *(struct opt_t *)data;
	int fd_run[odb.nr_run];
	struct MergeArgs merge_args[odb.nr_merge_th];
	pthread_t mrg_thread[odb.nr_merge_th];

	for(int i = 0; i < odb.nr_run; i++){
		fd_run[i] = open( (odb.runpath + std::to_string(i)).c_str(), O_DIRECT | O_RDONLY);
	}

	uint64_t *range_table;
	range_table = (uint64_t *)calloc(odb.nr_merge_th * odb.nr_run, sizeof(uint64_t));

	file_to_range(&range_table[0], odb);


	for(int th_id = 0; th_id < odb.nr_merge_th; th_id++){
		int nr_entries = 0;
		merge_args[th_id].th_id = th_id;
		merge_args[th_id].nr_run = odb.nr_run;
		merge_args[th_id].nr_range = odb.nr_merge_th;
		merge_args[th_id].blk_size = odb.mrg_blksize;
		merge_args[th_id].wrbuf_size = odb.mrg_wrbuf;
		merge_args[th_id].fd_run = fd_run;
		merge_args[th_id].range_table = &range_table[th_id * odb.nr_run];
		for(int i = 0; i < odb.nr_run; i++){
			nr_entries += range_table[th_id * odb.nr_run + i];
		}
		merge_args[th_id].nr_entries = nr_entries;
		merge_args[th_id].outpath = odb.outpath + std::to_string(th_id);
		pthread_create(&mrg_thread[th_id], NULL, t_Merge, &merge_args[th_id]);
	}
	
	int is_ok = 0;
	int res = 0;
	for(int th_id = 0; th_id < odb.nr_merge_th; th_id++){
		pthread_join(mrg_thread[th_id], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}
	
	for(int i = 0; i < odb.nr_run; i++){
		close(fd_run[i]);
		
		int remove_res = remove( (odb.runpath + std::to_string(i)).c_str());
		assert(remove_res == 1);
	}
	
	free(range_table);	
}

