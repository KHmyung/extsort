#define _LARGEFILE63_SOURCE

#include "extsort.h"

#define NSTOS(ns)	(ns/1000000000)
#define NSTOMS(ns)	(ns/1000000)

static void
print_time(const char *name, unsigned long long time){
	std::cout << name << ": " << NSTOS(time) << std::endl; 
}

static void
print_thread_time(int id, struct TimeFormat time){
	std::cout << "  " << id << ": " << NSTOS(time.arrival_t[id]) << " "
				<< NSTOS(time.sort_t[id]) << " "
				<< NSTOS(time.read_t[id]) << " "
				<< NSTOS(time.write_t[id]) << std::endl;
}

static void
stat_threads(struct TimeStats *stats, struct TimeFormat src, int nr_thread){
	
	stats->first_time = src.arrival_t[0];
	stats->last_time = 0;
	stats->avg_total = 0;
	stats->avg_sort = 0;
	stats->avg_read = 0;
	stats->avg_write = 0;
	
	for (int th = 0; th < nr_thread; th++){
		if(stats->last_time < src.arrival_t[th]){
			stats->last_time = src.arrival_t[th];
		}
		if(stats->first_time > src.arrival_t[th]){
			stats->first_time = src.arrival_t[th];
		}
		stats->avg_total += src.arrival_t[th];
		stats->avg_sort += src.sort_t[th];
		stats->avg_read += src.read_t[th];
		stats->avg_write += src.write_t[th];
	}

	stats->avg_total /= nr_thread;
	stats->avg_sort /= nr_thread;
	stats->avg_read /= nr_thread;
	stats->avg_write /= nr_thread;
}

void 
ShowStats(void* data){
	struct opt_t odb = *(struct opt_t *)data;
	struct TimeStats run_stat;
	struct TimeStats mrg_stat;

	stat_threads(&run_stat, run_time, odb.nr_runform_th);
	stat_threads(&mrg_stat, mrg_time, odb.nr_merge_th);

	std::cout << "\n<PROFILE>" << std::endl;
	
	print_time(" Total        ", run_time.total_t + mrg_time.total_t);
	print_time(" RunFormation ", run_time.total_t);
	print_time(" Merge        ", mrg_time.total_t);
	
	std::cout << "\n<STATS>" << std::endl;
	print_time("  AVG  Total        ", run_stat.avg_total + mrg_stat.avg_total);
	print_time("  AVG  RunFormation ", run_stat.avg_total);
	print_time("       Sort         ", run_stat.avg_sort);
	print_time("       Read         ", run_stat.avg_read);
	print_time("       Write        ", run_stat.avg_write);
	print_time(" FIRST RunFormation ", run_stat.first_time);
	print_time("  LAST RunFormation ", run_stat.last_time);
	print_time("  AVG  Merge        ", mrg_stat.avg_total);
	print_time("       Sort         ", mrg_stat.avg_sort);
	print_time("       Read         ", mrg_stat.avg_read);
	print_time("       Write        ", mrg_stat.avg_write);
	print_time(" FIRST Merge        ", mrg_stat.first_time);
	print_time("  LAST Merge        ", mrg_stat.last_time);
	
	std::cout << "\n<THREADS STATS | ";
	std::cout << "(id) | (Total),(Sort),(Read),(Write)>" << std::endl;	
	std::cout << "Run Formation" << std::endl;
	for (int th = 0; th < odb.nr_runform_th; th++){
		print_thread_time(th, run_time);	
	}
	std::cout << "Merge" << std::endl;
	for (int th = 0; th < odb.nr_merge_th; th++){
		print_thread_time(th, mrg_time);	
	}
}

int 
main(void){

	int res = 0;
	
	struct timespec local_time[2];

	struct opt_t* opt;
	opt = (struct opt_t *)calloc(1, sizeof(struct opt_t));
	assert(opt != NULL);

	opt_init(opt);
	opt_print(opt);
	
	std::cout << "\n<START TEST>"	<< std::endl;
#if DO_DATAGEN
	std::cout << "DATA GENERATION" << std::endl;
	DataGeneration(opt);
#endif

#if DO_RUNFORM
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "RUN FORMATION" << std::endl;
	RunFormation(opt);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &run_time.total_t, &run_time.total_c);
#endif

#if DO_MERGE	
	clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
	std::cout << "MERGING" << std::endl;
        Merge(opt);
	clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
	calclock(local_time, &mrg_time.total_t, &mrg_time.total_c);
#endif
	std::cout << "<TEST FINISHED>" << std::endl;	

	ShowStats(opt);
	free(opt);
	return 0; 
}
