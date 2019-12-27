#define _LARGEFILE63_SOURCE

#include "external_sort.h"
#include "profile.h"
#include "debug.h"

int GenerateDataFile(){
	int fd = open( INPUT_PATH, O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
	if(fd == -1){
		fprintf(stderr, "FAIL: open file(%s) - %s\n", INPUT_PATH, strerror(errno));
	}
	pthread_t p_thread[NRTH_DATAGEN];
	int thread_id[NRTH_DATAGEN];
	struct DatagenArgs datagen_args[NRTH_DATAGEN];
	
	for(int i=0; i<NRTH_DATAGEN; i++){
		datagen_args[i].fd_input = fd;
		datagen_args[i].th_idx = i;
		datagen_args[i].writeofs = i*(NR_ENTRIES/NRTH_DATAGEN)*DATA_SIZE;
		
		thread_id[i] = pthread_create(&p_thread[i], NULL, t_GenerateDataFile, (void*)&datagen_args[i]);
	}
	
	int is_ok = 0;
	int res = 0;
	for(int i=0; i<NRTH_DATAGEN; i++){
		pthread_join(p_thread[i], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}

	close(fd);
	return 1;
}

void* t_GenerateDataFile(void* data){

	struct DatagenArgs args = *(struct DatagenArgs*)data;
	int fd_input = args.fd_input;
	int th_idx = args.th_idx;
	unsigned int seed = (unsigned int)th_idx;
	uint64_t writeofs = args.writeofs;

	Data *genbuf = alloc_buf();
	
	int64_t nbyte_buffer = 0;
	int64_t nbyte_written = 0;
	int64_t offset = 0;
	
	for(size_t i=0; i<NR_ENTRIES/NRTH_DATAGEN; i++){

		randstring(sizeof(genbuf[offset].value), genbuf[offset].value, (unsigned int)seed);
		genbuf[offset].key = rand_r((unsigned int*)&seed) % KEYRANGE;

		offset++;
		nbyte_buffer += DATA_SIZE;

		if(nbyte_buffer == MEM_SIZE/NRTH_DATAGEN){

			writeofs = pWriteData(fd_input, (char *)genbuf, nbyte_buffer, writeofs);
			
			nbyte_written += nbyte_buffer;
			nbyte_buffer = 0;
			offset = 0;
		}
	}

	free(genbuf);
}

int RunFormation(){
	
	run_idx = 0;
	
	pthread_t p_thread[NRTH_RUNFORM];
	int thread_id[NRTH_RUNFORM];
	struct RunformationArgs runformation_args[NRTH_RUNFORM];

	int fd_input = open( INPUT_PATH, O_RDONLY | O_DIRECT);
	if(fd_input == -1){
		fprintf(stderr, "FAIL: open file - %s\n", strerror(errno));
	}
	
	off_t cumulative_offset = 0;
	for(int i=0; i<NRTH_RUNFORM; i++){
		runformation_args[i].fd_input = fd_input;
		runformation_args[i].th_idx = i;
		runformation_args[i].st_offset = cumulative_offset;
		runformation_args[i].r_buffer = (char *)g_buffer + cumulative_offset;
		runformation_args[i].nbyte_load = TOTAL_DATA_SIZE / NRTH_RUNFORM;
		
		thread_id[i] = pthread_create(&p_thread[i], NULL, t_RunFormation, (void*)&runformation_args[i]);
		
		cumulative_offset += BUFFER_SIZE;
	}
	
	int is_ok = 0;
	int res = 0;
	for(int i=0; i<NRTH_RUNFORM; i++){
		pthread_join(p_thread[i], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}

	for(int range = 0; range < NRTH_RANGEMRG; range++){
		for(int run = 0; run < NR_RUNS; run++){
			g_keydist[range] += keydist[run][range];			
//			std::cout << "[FILE" << run << "][RANGE" << range << "]: " << keydist[run][range] << std::endl;
		}
		std::cout << "[RANGE TOTAL" << range << "]: " << g_keydist[range] << std::endl;
	}
	close(fd_input);

	return res;
}

void* t_RunFormation(void *data){

	struct RunformationArgs args = *(struct RunformationArgs*)data;
	int fd_input = args.fd_input;
	int th_idx = args.th_idx;
	char *r_buffer = args.r_buffer;
	Data *g_buffer__ = (Data *)r_buffer;
	int64_t nbyte_load = args.nbyte_load;
	int64_t nbyte_read = 0;
	struct timespec local_time[2];
	struct timespec thread_time[2];
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
#endif

#if DO_TBB
	tbb::task_scheduler_init init(NRTH_TBB);
#endif

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
#if SET_AFFINITY
        CPU_SET(th_idx%NR_CORE, &cpuset);

#elif SET_NUMA
        CPU_SET(queue_mask[th_idx%NR_CORE]-1, &cpuset);
#endif

#if (SET_AFFINITY || SET_NUMA)
	if(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)<0){
		std::cout << "pthread error" <<std::endl;
	}	
#endif
	
	while(nbyte_read < nbyte_load){
		int run = run_idx++;
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
		int64_t tmp_byte_read = ReadData(fd_input, r_buffer, BUFFER_SIZE);
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &runform_read_time[th_idx], &runform_read_count[th_idx]);
#endif

	if(tmp_byte_read == -1){
			fprintf(stderr, "FAIL: read - %s\n", strerror(errno));
			return ((void *)-1);
		}
		nbyte_read += tmp_byte_read;
		uint64_t dist[NR_FILTER] = { 0, };
		
#if DO_TBB
		tbb::parallel_sort(&g_buffer__[0], &g_buffer__[0] + NR_ENTRIES_BUFFER, compare);
#else
		std::sort(&g_buffer__[0], &g_buffer__[0] + NR_ENTRIES_BUFFER, compare);
#endif
		std::string run_path = RUN_PATH;
		run_path += "run_" + std::to_string(run);
		
		range_balancing(g_buffer__, NR_ENTRIES_BUFFER, run);
		int fd_run = open( run_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
		if(fd_run == -1){
			fprintf(stderr, "FAIL: open file(%s) - %s\n", run_path.c_str() , strerror(errno));
			return ((void *)-1);
		}
	
	
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
        int64_t tmp_byte_write = WriteData(fd_run, (char *)g_buffer__, BUFFER_SIZE);
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &runform_write_time[th_idx], &runform_write_count[th_idx]);
#endif
	
        if(tmp_byte_write == -1){
			fprintf(stderr, "FAIL: write - %s\n", strerror(errno));
			return ((void *)-1);
		}
		close(fd_run);
	}
#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
	calclock(thread_time, &runform_arrival_time[th_idx], &runform_arrival_count[th_idx]);
	runform_sort_time[th_idx] = runform_arrival_time[th_idx] - runform_read_time[th_idx] - runform_write_time[th_idx];
#endif

	return ((void *)1);
}
	
void RangeMerge(){
		
	std::string inpath = RUN_PATH;
	inpath += "run_";
	int nrfiles = NR_RUNS;
	
	struct RangeMergeArgs merge_args[NRTH_RANGEMRG];

	for(int i = 0; i < nrfiles; i++){
		fd_mrgs[i] = open( (inpath + std::to_string(i)).c_str(), O_DIRECT | O_RDONLY);
	}

	pthread_t mrg_thread[NRTH_RANGEMRG];

	for(int th_idx = 0; th_idx < NRTH_RANGEMRG; th_idx++){
		merge_args[th_idx].th_idx = th_idx;
		if(g_keydist[th_idx] == 0) continue;
		pthread_create(&mrg_thread[th_idx], NULL, t_RangeMerge, &merge_args[th_idx]);
	}
	
	int is_ok = 0;
	int res = 0;
	for(int i = 0; i < NRTH_RANGEMRG; i++){
		pthread_join(mrg_thread[i], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}
	
	for(int i = 0; i < nrfiles; i++){
		close(fd_mrgs[i]);
		
		int remove_res = remove( (inpath + std::to_string(i)).c_str());
		if(remove_res == -1){
			fprintf(stderr, "FAIL: remove file - %s\n", strerror(errno));
		}
		
	}
}

void *t_RangeMerge(void* data){
	int th_cnt = NRTH_RANGEMRG;		// 32
	int nr_files = NR_RUNS;		// 32
	uint64_t runsize[nr_files];
	
	struct RangeMergeArgs args = *(struct RangeMergeArgs*)data;
	int th_idx = args.th_idx;
	struct timespec thread_time[2];

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[0]);
#endif
	
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
#if SET_AFFINITY
        CPU_SET(th_idx%NR_CORE, &cpuset);
#elif SET_NUMA
        CPU_SET(queue_mask[th_idx%NR_CORE]-1, &cpuset);
#endif

#if (SET_AFFINITY || SET_NUMA)
	if(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset)<0){
		std::cout << "pthread error" <<std::endl;
	}	
#endif

	std::string out_path = OUTPUT_PATH;
	out_path += "range_" + std::to_string(th_idx);
		
	int fd_output = open( out_path.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_LARGEFILE, 0644);
	
	uint64_t blksize = (MEM_SIZE/th_cnt)/nr_files;	// 128KB
	uint64_t blkentry[nr_files];
	uint64_t mrgsize = MRG_SIZE;	// 128KB
	uint64_t mrgentry = mrgsize/DATA_SIZE;	// 16*1024
	uint64_t nr_blk[nr_files] = {0, };
	std::priority_queue<idx_Data, std::vector<idx_Data>, std::greater<idx_Data>> pq;
	Data *blkbuf = alloc_buf();
	Data *mrgbuf = alloc_buf();
	Data *tmpbuf = alloc_buf();
	idx_Data slot;
	
	off_t mrgofs = 0;
	off_t blkofs[nr_files];
	uint64_t remainder[nr_files];
	struct timespec local_time[2];

	off_t w_fileofs = 0;
	off_t writeofs = 0;
	uint64_t lastmrg = (g_keydist[th_idx]*DATA_SIZE)%mrgsize;

	off_t read_byteofs;
	off_t readofs[nr_files];
	off_t endbyte[nr_files];

	for(int file = 0; file < nr_files; file++){
		read_byteofs = 0;
		for(int range = 0; range < th_idx; range++){
			read_byteofs += keydist[file][range]; // read offset from each run file
		}
		read_byteofs *= DATA_SIZE;
		readofs[file] = read_byteofs/blksize;
		readofs[file] *= blksize;
		blkofs[file] = (read_byteofs%blksize)/DATA_SIZE; // initialize block offset of each range
		blkentry[file] = blksize/DATA_SIZE;
	
		endbyte[file] = read_byteofs+(keydist[file][th_idx]*DATA_SIZE);
		nr_blk[file] = endbyte[file]/blksize - readofs[file]/blksize + 1;
		remainder[file] = (endbyte[file]%blksize)/DATA_SIZE;	// remaining data entries from last block
		if(remainder[file] == 0) nr_blk[file]--;
	}
	off_t runofs[nr_files] = {0, };
	for(int file = 0; file < nr_files; file++){
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif		
	readofs[file] = pReadData(fd_mrgs[file], (char*)blkbuf + blksize*file, blksize, readofs[file]);
#if DO_PROFILE
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &merge_read_time[th_idx], &merge_read_count[th_idx]);
#endif
		runofs[file]++;
		
		if(readofs[file] == -1){
			fprintf(stderr, "FAIL: read- %s\n", strerror(errno));
		}

		slot.idx = file;
		slot.data = blkbuf[(file*blksize)/DATA_SIZE + blkofs[file]++];
		pq.push(slot);
	}

#if DO_VERIFY	
	std::cout << "min: " << pq.top().data.key << "_" << th_idx << std::endl;
#endif
	uint64_t tgt_merge = g_keydist[th_idx];
	uint64_t nbyte_merge = 0;

	while(true){
		slot = pq.top();
		mrgbuf[mrgofs++] = slot.data;
		if(++nbyte_merge > tgt_merge) break;
		pq.pop();

#if DO_VERIFY
		if(blkofs[slot.idx] > blkentry[slot.idx])
			std::cout << "offset overflow: " << blkofs[slot.idx] << "(" << th_idx << ")" << std::endl;
		if(slot.data.key==0 && th_idx!=0){
			std::cout << "ZERO!! TH"<< th_idx << " in runfile(" << slot.idx << ")" << "\nmrgofs= " << mrgofs << "\nnbyte_merge= " << nbyte_merge << "/" << tgt_merge << "\nrunofs= " << runofs[slot.idx] << "\nmrgentry= " << mrgentry << "\nblkofs= " << blkofs[slot.idx] << "\nnrblks= " << nr_blk[slot.idx] << "\nreadofs= " << readofs[slot.idx] << "\nblkentry= " << blkentry[slot.idx] << std::endl;
		}
#endif
		if(mrgofs == mrgentry){
#if DO_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
			WriteData(fd_output, (char*)mrgbuf, mrgsize);
#if DO_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
				calclock(local_time, &merge_write_time[th_idx], &merge_write_count[th_idx]);
#endif
			mrgofs = 0;
		}
		
		if(blkofs[slot.idx] == blkentry[slot.idx]){
			if(runofs[slot.idx] < nr_blk[slot.idx]){
#if DO_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif
			readofs[slot.idx] = pReadData(fd_mrgs[slot.idx], (char*)blkbuf + blksize*slot.idx, blksize, readofs[slot.idx]);
#if DO_PROFILE
				clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
				calclock(local_time, &merge_read_time[th_idx], &merge_read_count[th_idx]);
#endif
				blkofs[slot.idx] = 0;
				if((runofs[slot.idx] == nr_blk[slot.idx]-1) && remainder[slot.idx]){
					blkentry[slot.idx] = remainder[slot.idx];
				}
				runofs[slot.idx]++;
			}
			else continue;
		}
		slot.data = blkbuf[(slot.idx*blksize)/DATA_SIZE + blkofs[slot.idx]++];
		pq.push(slot);
	}

#if DO_VERIFY	
	std::cout << "max: " << mrgbuf[0].key << "_" << th_idx << std::endl;
#endif

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
#endif

	WriteData(fd_output, (char*)mrgbuf, mrgsize);

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &merge_write_time[th_idx], &merge_write_count[th_idx]);
#endif

#if DO_PROFILE
	clock_gettime(CLOCK_MONOTONIC, &thread_time[1]);
	calclock(thread_time, &merge_arrival_time[th_idx], &merge_arrival_count[th_idx]);
	merge_sort_time[th_idx] = merge_arrival_time[th_idx] - merge_read_time[th_idx] - merge_write_time[th_idx];
#endif
	
	close(fd_output);

	free(blkbuf);
	free(mrgbuf);
	free(tmpbuf);
}

void PrintStat(){
	int runform_first =0;
	int runform_last =0;
        int merge_last =0;
        int merge_first =0;			
	unsigned long long last_arrival = 0;		// time value
	unsigned long long first_arrival = 0;		// time value
	unsigned long long rf_arrival = 0;		// sum and avg
	unsigned long long rf_read = 0;			// sum and avg
	unsigned long long rf_write = 0;		// sum and avg
	unsigned long long rf_sort = 0;			// sum and avg
	unsigned long long mrg_arrival = 0;		// sum and avg
	unsigned long long mrg_read = 0;		// sum and avg
	unsigned long long mrg_write = 0;		// sum and avg
	unsigned long long mrg_sort = 0;		// sum and avg
	int nrth_mrg = 0;

	std::cout << "------------------TOTAL TIME(s)---------------------" << std::endl;
	std::cout << "TOTAL: " << (runformation_time +  merge_time)/1000000000 << std::endl;
	std::cout << "RUNFORMATION: " << runformation_time/1000000000 << std::endl;
	std::cout << "RANGE_MERGE: " << merge_time/1000000000 << std::endl;

#if DO_PROFILE
	std::cout << "-----------------DETAILED PROFILE-------------------" << std::endl;
	first_arrival = runform_arrival_time[0];
	for (int th = 0; th < NRTH_RUNFORM; th++){		// runformation profile
		if(last_arrival < runform_arrival_time[th]){
			last_arrival = runform_arrival_time[th];
			runform_last = th;
		}
		if(first_arrival > runform_arrival_time[th]){
			first_arrival = runform_arrival_time[th];
			runform_first = th;
		}
		rf_arrival += runform_arrival_time[th];
		rf_read += runform_read_time[th];
		rf_write += runform_write_time[th];
		rf_sort += runform_sort_time[th];
	}
	rf_arrival = rf_arrival/NRTH_RUNFORM;
	rf_read = rf_read/NRTH_RUNFORM;
	rf_write = rf_write/NRTH_RUNFORM;
	rf_sort = rf_sort/NRTH_RUNFORM;
	last_arrival = 0;
	first_arrival = merge_arrival_time[0];

	if(NRTH_RANGEMRG >= 16){
		nrth_mrg = NRTH_RANGEMRG-1;
	}else nrth_mrg = NRTH_RANGEMRG;

	for (int th = 0; th < nrth_mrg; th++){		// merge profile
		if(last_arrival < merge_arrival_time[th]){
			last_arrival = merge_arrival_time[th];
			merge_last = th;
		}
		if(first_arrival > merge_arrival_time[th]){
			first_arrival = merge_arrival_time[th];
			merge_first = th;
		}
		mrg_arrival += merge_arrival_time[th];
		mrg_read += merge_read_time[th];
		mrg_write += merge_write_time[th];
		mrg_sort += merge_sort_time[th];
	}
	mrg_arrival = mrg_arrival/nrth_mrg;
	mrg_read = mrg_read/nrth_mrg;
	mrg_write = mrg_write/nrth_mrg;
	mrg_sort = mrg_sort/nrth_mrg;

	std::cout << "[RF-FIRST-ARRIVAL-TH" << runform_first << "] " << runform_arrival_time[runform_first]/1000000000 << std::endl;
	std::cout << "[RF-FIRST-READ-TH" << runform_first << "] " << runform_read_time[runform_first]/1000000000 << std::endl;
	std::cout << "[RF-FIRST-WRITE-TH" << runform_first << "] " << runform_write_time[runform_first]/1000000000 << std::endl;
	std::cout << "[RF-FIRST-SORT-TH" << runform_first << "] " << runform_sort_time[runform_first]/1000000000 << std::endl;
	std::cout << "[RF-LAST-ARRIVAL-TH" << runform_last << "] " << runform_arrival_time[runform_last]/1000000000 << std::endl;
	std::cout << "[RF-LAST-READ-TH" << runform_last << "] " << runform_read_time[runform_last]/1000000000 << std::endl;
	std::cout << "[RF-LAST-WRITE-TH" << runform_last << "] " << runform_write_time[runform_last]/1000000000 << std::endl;
	std::cout << "[RF-LAST-SORT-TH" << runform_last << "] " << runform_sort_time[runform_last]/1000000000 << std::endl;
	std::cout << "[RF-AVG-ARRIVAL] " << rf_arrival/1000000000 << std::endl;
	std::cout << "[RF-AVG-READ] " << rf_read/1000000000 << std::endl;
	std::cout << "[RF-AVG-WRITE] " << rf_write/1000000000 << std::endl;
	std::cout << "[RF-AVG-SORT] " << rf_sort/1000000000 << std::endl;

	std::cout << "[MRG-FIRST-ARRIVAL-TH" << merge_first << "] " << merge_arrival_time[merge_first]/1000000000 << std::endl;
	std::cout << "[MRG-FIRST-READ-TH" << merge_first << "] " << merge_read_time[merge_first]/1000000000 << std::endl;
	std::cout << "[MRG-FIRST-WRITE-TH" << merge_first << "] " << merge_write_time[merge_first]/1000000000 << std::endl;
	std::cout << "[MRG-FIRST-SORT-TH" << merge_first << "] " << merge_sort_time[merge_first]/1000000000 << std::endl;
	std::cout << "[MRG-LAST-ARRIVAL-TH" << merge_last << "] " << merge_arrival_time[merge_last]/1000000000 << std::endl;
	std::cout << "[MRG-LAST-READ-TH" << merge_last << "] " << merge_read_time[merge_last]/1000000000 << std::endl;
	std::cout << "[MRG-LAST-WRITE-TH" << merge_last << "] " << merge_write_time[merge_last]/1000000000 << std::endl;
	std::cout << "[MRG-LAST-SORT-TH" << merge_last << "] " << merge_sort_time[merge_last]/1000000000 << std::endl;
	std::cout << "[MRG-AVG-ARRIVAL] " << mrg_arrival/1000000000 << std::endl;
	std::cout << "[MRG-AVG-READ] " << mrg_read/1000000000 << std::endl;
	std::cout << "[MRG-AVG-WRITE] " << mrg_write/1000000000 << std::endl;
	std::cout << "[MRG-AVG-SORT] " << mrg_sort/1000000000 << std::endl;

	for (int th = 0; th < NRTH_RUNFORM; th++){		
		std::cout << "[RF-READ-TH" << th << "] " << runform_read_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RUNFORM; th++){		
		std::cout << "[RF-WRITE-TH" << th << "] " << runform_write_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RUNFORM; th++){		
		std::cout << "[RF-SORT-TH" << th << "] " << runform_sort_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RUNFORM; th++){		
		std::cout << "[RF-ARRIVAL-TH" << th << "] " << runform_arrival_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RANGEMRG; th++){		
		std::cout << "[MRG-READ-TH" << th << "] " << merge_read_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RANGEMRG; th++){		
		std::cout << "[MRG-WRITE-TH" << th << "] " << merge_write_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RANGEMRG; th++){		
		std::cout << "[MRG-SORT-TH" << th << "] " << merge_sort_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_RANGEMRG; th++){		
		std::cout << "[MRG-ARRIVAL-TH" << th << "] " << merge_arrival_time[th]/1000000000 << std::endl;
	}
#endif
	
}

void PrintConfig(){
	std::cout << "TOTAL_DATA_SIZE: " << TOTAL_DATA_SIZE/1024/1024/1024 << "GB" << std::endl;
	std::cout << "DATA_SIZE: " << DATA_SIZE << "B" << std::endl;
	std::cout << "MEM_SIZE: " << MEM_SIZE/1024/1024 << "MB" << std::endl;
	std::cout << "NRTH_RUNFORM: " << NRTH_RUNFORM << std::endl;
	std::cout << "NRTH_RANGEMRG: " << NRTH_RANGEMRG << std::endl;
	std::cout << "NR_RUNS: " << NR_RUNS << std::endl;
	std::cout << "READBLK(MRG): " << MEM_SIZE/NRTH_RANGEMRG/NR_RUNS << std::endl;
}

int main(int argc, char* argv[]){

	int res = 0;
	
	struct timespec local_time[2];

	g_buffer = alloc_buf();
	PrintConfig();
#if DO_DATAGEN
	std::cout << "START DATA GENERATION" << std::endl;
	GenerateDataFile();
#endif

#if DO_RUNFORM
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "START RUNFORMATION" << std::endl;
	RunFormation();
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &runformation_time, &runformation_count);
	std::cout << "[RUNFORMATION: " << runformation_time/1000000000 << "s]" << std::endl;
#endif

#if DO_GROUP_MERGE
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "START GROUP_MERGE" << std::endl;
        GroupMerge();
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &reduce_time, &reduce_count);
	std::cout << "[GROUP_MERGE: " << reduce_time/1000000000 << "s]" << std::endl;
#endif

#if DO_RANGE_MERGE	
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "START RANGE_MERGE" << std::endl;
        RangeMerge();
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &merge_time, &merge_count);
	std::cout << "[RANGE_MERGE: " << merge_time/1000000000 << "s]" << std::endl;
#endif
	
	PrintStat();
	free(g_buffer);
	return 0; 
}
