#define _LARGEFILE63_SOURCE

#include "extsort.h"

#define NSTOS(ns)	(ns/1000000000)

struct time_profile mytime;

static struct Data* alloc_buf(int64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
}


void PrintStat(void* data){
	/*
	struct opt_t odb = *(struct opt_t *)data;
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
	for (int th = 0; th < odb.nr_runform_th; th++){		// runformation profile
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
	rf_arrival = rf_arrival/odb.nr_runform_th;
	rf_read = rf_read/odb.nr_runform_th;
	rf_write = rf_write/odb.nr_runform_th;
	rf_sort = rf_sort/odb.nr_runform_th;
	last_arrival = 0;
	first_arrival = merge_arrival_time[0];

	if(NRTH_MRG >= 16){
		nrth_mrg = NRTH_MRG-1;
	}else nrth_mrg = NRTH_MRG;

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

	for (int th = 0; th < odb.nr_runform_th; th++){		
		std::cout << "[RF-READ-TH" << th << "] " << runform_read_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < odb.nr_runform_th; th++){		
		std::cout << "[RF-WRITE-TH" << th << "] " << runform_write_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < odb.nr_runform_th; th++){		
		std::cout << "[RF-SORT-TH" << th << "] " << runform_sort_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < odb.nr_runform_th; th++){		
		std::cout << "[RF-ARRIVAL-TH" << th << "] " << runform_arrival_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_MRG; th++){		
		std::cout << "[MRG-READ-TH" << th << "] " << merge_read_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_MRG; th++){		
		std::cout << "[MRG-WRITE-TH" << th << "] " << merge_write_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_MRG; th++){		
		std::cout << "[MRG-SORT-TH" << th << "] " << merge_sort_time[th]/1000000000 << std::endl;
	}
	for (int th = 0; th < NRTH_MRG; th++){		
		std::cout << "[MRG-ARRIVAL-TH" << th << "] " << merge_arrival_time[th]/1000000000 << std::endl;
	}
#endif
	*/
}

int main(void){

	int res = 0;
	
	struct timespec local_time[2];

	struct opt_t* opt;
	opt = (struct opt_t *)calloc(1, sizeof(struct opt_t));
	assert(opt != NULL);

	opt_init(opt);
	opt_print(opt);
	
#if DO_DATAGEN
	std::cout << "DATA GENERATION" << std::endl;
	DataGeneration(opt);
#endif

#if DO_RUNFORM
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "RUNFORMATION" << std::endl;
	RunFormation(opt);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &run_time.runform_t, &run_time.runform_c);
	std::cout << "[RUNFORMATION: " << NSTOS(run_time.runform_t) << "s]" << std::endl;
#endif

#if DO_MERGE	
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "MERGE" << std::endl;
        Merge(opt);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &mrg_time.merge_t, &mrg_time.merge_c);
	std::cout << "[MERGE: " << NSTOS(mrg_time.merge_t) << "s]" << std::endl;
#endif
	
	//PrintStat();
	free(opt);
	return 0; 
}
