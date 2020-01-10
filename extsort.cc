#define _LARGEFILE63_SOURCE

#include "extsort.h"

#define NSTOS(ns)	(ns/1000000000)
#define NSTOMS(ns)	(ns/1000000)

static void
print_time(const char *name, unsigned long long time){
	std::cout << name << ": " << NSTOS(time) << "."
		<< NSTOMS(time)%1000 << std::endl;
}

static void
print_thread_time(int id, struct TimeFormat time){
	std::cout << " |  TH " << id << "  | "
				<< NSTOS(time.arrival_t[id]) << " "
				<< NSTOS(time.sort_t[id]) << " "
				<< NSTOS(time.read_t[id]) << " "
				<< NSTOS(time.write_t[id]) << std::endl;
}

static void
clear_input(std::string *path, int nr_files){

	for(int i = 0; i < nr_files; i++){

		int res = remove(path[i].c_str());
		assert( res == 0 );
	}
}

static void
clear_run(std::string *path, int nr_files, int nr_range){

	for(int i = 0; i < nr_range; i++){
		for(int j = 0; j < nr_files; j++){

			int res = remove( (path[i] + std::to_string(j)).c_str() );
			assert( res == 0 );
		}
	}
}

static void
clear_output(std::string *path, int nr_files){

	for(int i = 0; i < nr_files; i++){

		int res = remove((path[i] + std::to_string(i)).c_str());
		assert( res == 0 );
	}
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

	if(odb.runform)
		stat_threads(&run_stat, run_time, odb.nr_runform_th);
	if(odb.merge)
		stat_threads(&mrg_stat, mrg_time, odb.nr_merge_th);

	std::cout << "\n <PROFILE>" << std::endl;

	if(odb.runform && odb.merge)
		print_time("    Total(s)        ", run_time.total_t + mrg_time.total_t);
	if(odb.runform)
		print_time("    RunFormation(s) ", run_time.total_t);
	if(odb.merge)
		print_time("    Merge(s)        ", mrg_time.total_t);

	if(do_profile){
		std::cout << "\n <STATS>" << std::endl;
		if(odb.runform && odb.merge){
			std::cout << "  [-----TOTAL(s)----] " << std::endl;
			print_time("  AVG  Total        ", run_stat.avg_total + mrg_stat.avg_total);
			print_time("       -Sort        ", run_stat.avg_sort + mrg_stat.avg_sort);
			print_time("       -Read        ", run_stat.avg_read + mrg_stat.avg_read);
			print_time("       -Write       ", run_stat.avg_write + mrg_stat.avg_write);
		}
		if(odb.runform){
			std::cout << "  [-RUNFORMATION(s)-] " << std::endl;
			print_time(" FIRST Arrival      ", run_stat.first_time);
			print_time("  LAST Arrival      ", run_stat.last_time);
			print_time("  AVG  Total        ", run_stat.avg_total);
			print_time("       -Sort        ", run_stat.avg_sort);
			print_time("       -Read        ", run_stat.avg_read);
			print_time("       -Write       ", run_stat.avg_write);
		}
		if(odb.merge){
			std::cout << "  [-----MERGE(s)----] " << std::endl;
			print_time(" FIRST Arrival      ", mrg_stat.first_time);
			print_time("  LAST Arrival      ", mrg_stat.last_time);
			print_time("  AVG  Total        ", mrg_stat.avg_total);
			print_time("       -Sort        ", mrg_stat.avg_sort);
			print_time("       -Read        ", mrg_stat.avg_read);
			print_time("       -Write       ", mrg_stat.avg_write);
		}

		std::cout << "\n <THREADS STATS>\n ";
		std::cout << "|  (id)  | (Total) (Sort) (Read) (Write)>" << std::endl;
		if(odb.runform){
			std::cout << " [-RUNFORMATION(s)-] " << std::endl;
			for (int th = 0; th < odb.nr_runform_th; th++){
				print_thread_time(th, run_time);
			}
		}
		if(odb.merge){
			std::cout << " [---- MERGE(s)----] " << std::endl;
			for (int th = 0; th < odb.nr_merge_th; th++){
				print_thread_time(th, mrg_time);
			}
		}
	}
}

static void
free_timestruct(struct TimeFormat *time){

		free(time->arrival_t);
		free(time->arrival_c);
		free(time->read_t);
		free(time->read_c);
		free(time->write_t);
		free(time->write_c);
		free(time->sort_t);
		free(time->sort_c);
}


int
main(int argc, char *argv[]){

	int res = 0;

	struct timespec local_time[2];

	struct opt_t* opt;
	opt = (struct opt_t *)calloc(1, sizeof(struct opt_t));
	assert(opt != NULL);

	opt_init(opt);
	opt_parse(argc, argv, opt);
	opt_print(opt);

	std::cout << "\n <START TEST>"	<< std::endl;

	if(opt->datagen){		/* data generation */
		std::cout << "\n DATA GENERATION" << std::endl;
		DataGeneration(opt);
	}

	if(opt->runform){		/* runformation */
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
		std::cout << "\n RUN FORMATION" << std::endl;
		RunFormation(opt);
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &run_time.total_t, &run_time.total_c);
	}

	if(opt->runform && do_clear > 1){		/* deleting input file */
		clear_input(&opt->d_inpath[0], opt->nr_datagen_th);
	}

	if(opt->merge){			/* merge */
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
		std::cout << "\n MERGING" << std::endl;
		Merge(opt);
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &mrg_time.total_t, &mrg_time.total_c);
		std::cout << "\n <TEST FINISHED>" << std::endl;
	}

	if(opt->merge && do_clear > 0){		/* deleting run file */
		clear_run(&opt->d_runpath[0], opt->nr_run, opt->nr_merge_th);
	}

	if(opt->merge && do_clear > 2){
		clear_output(&opt->d_outpath[0], opt->nr_merge_th);
	}

	if(opt->runform || opt->merge)
		ShowStats(opt);
	free(opt);

	if(opt->runform)
		free_timestruct(&run_time);
	if(opt->merge)
		free_timestruct(&mrg_time);

	return 0;
}
