#define _LARGEFILE63_SOURCE

#include "extsort.h"

#define NSTOS(ns)	(ns/1000000000)
#define NSTOMS(ns)	(ns/1000000)

int do_profile;
int do_verify;
int do_clear;

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

	std::cout << "\n <PROFILE>" << std::endl;

	print_time(" Total        ", run_time.total_t + mrg_time.total_t);
	print_time(" RunFormation ", run_time.total_t);
	print_time(" Merge        ", mrg_time.total_t);

	std::cout << "\n <STATS>" << std::endl;
	std::cout << "     [ TOTAL ]     " << std::endl;
	print_time("  AVG  Total        ", run_stat.avg_total + mrg_stat.avg_total);
	std::cout << "\n  [ RUNFORMATION ] " << std::endl;
	print_time(" FIRST Arrival      ", run_stat.first_time);
	print_time("  LAST Arrival      ", run_stat.last_time);
	print_time("  AVG  Total        ", run_stat.avg_total);
	print_time("       -Sort        ", run_stat.avg_sort);
	print_time("       -Read        ", run_stat.avg_read);
	print_time("       -Write       ", run_stat.avg_write);
	std::cout << "\n     [ MERGE ]    " << std::endl;
	print_time(" FIRST Arrival      ", mrg_stat.first_time);
	print_time("  LAST Arrival      ", mrg_stat.last_time);
	print_time("  AVG  Total        ", mrg_stat.avg_total);
	print_time("       -Sort        ", mrg_stat.avg_sort);
	print_time("       -Read        ", mrg_stat.avg_read);
	print_time("       -Write       ", mrg_stat.avg_write);

	std::cout << "\n <THREADS STATS | ";
	std::cout << "(id) | (Total),(Sort),(Read),(Write)>" << std::endl;
	std::cout << "\n  [ RUNFORMATION ] " << std::endl;
	for (int th = 0; th < odb.nr_runform_th; th++){
		print_thread_time(th, run_time);
	}
	std::cout << "\n     [ MERGE ]    " << std::endl;
	for (int th = 0; th < odb.nr_merge_th; th++){
		print_thread_time(th, mrg_time);
	}
}

static char*
get_option(char **begin, char **end, const std::string &option){
	char ** itr = std::find(begin, end, option);
	if (itr != end && ++itr != end){
		return *itr;
	}
	return 0;
}

static std::string
char_to_str(char* a, int size){
	int i;
	std::string s = "";
	for (i = 0; i < size; i++){
		s += a[i];
	}
	return s;
}

static void
opt_parse(int argc, char *argv[], struct opt_t *odb){
	char *gen = get_option(argv, argv + argc, "-G");
	char *rnf = get_option(argv, argv + argc, "-R");
	char *mrg = get_option(argv, argv + argc, "-M");
	char *prf = get_option(argv, argv + argc, "-P");
	char *vrf = get_option(argv, argv + argc, "-V");
	char *clr = get_option(argv, argv + argc, "-C");
	char *tablepath = get_option(argv, argv + argc, "-t");
	char *inpath = get_option(argv, argv + argc, "-i");
	char *outpath = get_option(argv, argv + argc, "-o");
	char *runpath = get_option(argv, argv + argc, "-r");
	char *datasize = get_option(argv, argv + argc, "-d");
	char *memsize = get_option(argv, argv + argc, "-m");
	char *th = get_option(argv, argv + argc, "-w");

	if(gen){
		odb->datagen = atoi(gen);
	}
	if(rnf){
		odb->runform = atoi(rnf);
	}
	if(mrg){
		odb->merge = atoi(mrg);
	}
	if(vrf){
		do_verify = atoi(vrf);
	}
	if(prf){
		do_profile = atoi(prf);
	}
	if(clr){
		do_clear = atoi(clr);
	}
	if(tablepath){
		odb->metapath = char_to_str(tablepath, sizeof(tablepath)/sizeof(char));
	}
	if(inpath){
		odb->inpath = char_to_str(inpath, sizeof(inpath)/sizeof(char));
	}
	if(outpath){
		odb->outpath = char_to_str(outpath, sizeof(outpath)/sizeof(char));
	}
	if(runpath){
		odb->runpath = char_to_str(runpath, sizeof(runpath)/sizeof(char));
	}
	if(datasize){
		odb->total_size = ((uint64_t)atoi(datasize))*1024*1024*1024;
	}
	if(memsize){
		odb->mem_size = ((uint64_t)atoi(memsize))*1024*1024;
	}
	if(th){
		odb->nr_runform_th = atoi(th);
		odb->nr_merge_th = atoi(th);
	}
	if(datasize || memsize || th){
		odb->nr_run = (odb->total_size/odb->mem_size) * odb->nr_runform_th;
		assert(odb->nr_run > 0);
		odb->rf_blksize = (odb->mem_size/odb->nr_runform_th);
		assert(odb->rf_blksize >= 4096);
		odb->mrg_blksize = (odb->mem_size/odb->nr_merge_th/odb->nr_run);
		assert(odb->mrg_blksize >= 4096);
	}
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
	if(opt->datagen){
		std::cout << " DATA GENERATION" << std::endl;
		DataGeneration(opt);
	}

	if(opt->runform){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
		std::cout << " RUN FORMATION" << std::endl;
		RunFormation(opt);
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &run_time.total_t, &run_time.total_c);
	}

	if(do_clear == 2){	/* deleting input file */
		int res = remove(opt->inpath.c_str());
		assert ( res == 0 );
	}

	if(opt->merge){
		clock_gettime(CLOCK_MONOTONIC, &local_time[0]);
		std::cout << " MERGING" << std::endl;
		Merge(opt);
		clock_gettime(CLOCK_MONOTONIC, &local_time[1]);
		calclock(local_time, &mrg_time.total_t, &mrg_time.total_c);
		std::cout << " <TEST FINISHED>" << std::endl;
	}

	if(do_clear){		/* deleting run files */
		for(int i = 0; i < opt->nr_run; i++){
			int res =remove( (opt->runpath + std::to_string(i)).c_str());
			assert(res == 0);
		}
	}

	ShowStats(opt);
	free(opt);
	return 0;
}
