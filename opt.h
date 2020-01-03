#ifndef __OPT_H
#define __OPT_H

#include <iostream>

#define META_PATH "../meta.tmp"
#define INPUT_PATH "../input/in.txt"
#define OUTPUT_PATH "../output/"
#define RUN_PATH "../runs/"
#define TOTAL_DATA_SIZE (((int64_t)1*1024*1024*1024))
#define MEM_SIZE (((int64_t)128*1024*1024))

#define DO_DATAGEN false
#define DO_RUNFORM true
#define DO_MERGE true
#define DO_VERIFY false
#define DO_PROFILE true

#define NRTH_DATAGEN 4
#define NRTH_RUNFORM 4
#define NRTH_MRG 4

#define KV_SIZE (128)
#define MAXKEY 1073741824 // 0x40000000 1G(billion) key
#define NR_FILTER (NRTH_MRG*NRTH_MRG)
#define FILTER (MAXKEY/NR_FILTER)
#define NR_RANGE (NRTH_MRG)

#define RF_BUF_SIZE (MEM_SIZE / NRTH_RUNFORM)
#define MRG_BUF_SIZE (1*1024*1024)
#define BUFFER_SIZE (MEM_SIZE / NRTH_RUNFORM)
#define NR_RUNS ((TOTAL_DATA_SIZE / MEM_SIZE) * NRTH_RUNFORM)	// 1024
#define NR_ENTRIES_BUFFER (BUFFER_SIZE / KV_SIZE)
#define NR_ENTRIES (TOTAL_DATA_SIZE / KV_SIZE)
#define NR_ENTRIES_MRG (MRG_SIZE / KV_SIZE)


struct opt_t {
	bool datagen;
	bool runform;
	bool merge;
	std::string inpath;
	std::string outpath;
	std::string runpath;
	std::string metapath;
	int nr_datagen_th;
	int nr_runform_th;
	int nr_merge_th;
	uint64_t total_size;
	uint64_t mem_size;
	int nr_run;
	int kv_size;
	int key_size;
	int rf_blksize;
	int mrg_blksize;
	int mrg_wrbuf;
};

void opt_init(struct opt_t *);
void opt_parse(int, char**, struct opt_t *);
void opt_print(struct opt_t *);

extern int do_profile;
extern int do_verify;
extern int do_clear;

#endif /* OPT_H */
