#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "opt.h"

void
opt_init(struct opt_t *odb)				/* option database */
{
	odb->inpath = INPUT_PATH;			/* path to input file */
	odb->outpath = OUTPUT_PATH;			/* path to output files */
	odb->outpath += "range_";
	odb->runpath = RUN_PATH;			/* path to temporary run files */
	odb->runpath += "run_";
	odb->metapath = META_PATH;			/* path to temporary range metadata */
	odb->nr_datagen_th = NRTH_DATAGEN;		/* number of data generation threads */
	odb->nr_runform_th = NRTH_RUNFORM;		/* number of runformation threads */
	odb->nr_merge_th = NRTH_MRG;			/* number of merge threads */
	odb->total_size = TOTAL_DATA_SIZE;		/* total file size */
	odb->mem_size = MEM_SIZE;			/* total memory size */		
	odb->nr_run = 					/* number of run files */
		(odb->total_size/odb->mem_size)
				*odb->nr_runform_th;
	assert(odb->nr_run > 0);
	odb->kv_size = KV_SIZE;				/* key+value size */
	odb->key_size = /*8B*/ 8;			/* key size */
	odb->rf_blksize = 				/* available blkbuf per runformation thread */
		(odb->mem_size/odb->nr_runform_th);	
	odb->mrg_blksize =				/* available blkbuf per run file */
		(odb->mem_size/odb->nr_merge_th
		 /odb->nr_run);
	odb->mrg_wrbuf = /*1MB*/ 1*1024*1024;		/* available write buffer per merge thread */
}

void
opt_print(struct opt_t *odb)
{
	std::cout << "\n<CONFIGURATONS>" << std::endl;
	std::cout << "TOTAL_DATA_SIZE:	" << odb->total_size/1024/1024/1024 << "GB" << std::endl;
	std::cout << "MEM_SIZE:		" << odb->mem_size/1024/1024 << "MB" << std::endl;
	std::cout << "KV_SIZE(KEY" << odb->key_size << "B):		" << odb->kv_size << "B" << std::endl;
	std::cout << "NRTH_RUNFORM:		" << odb->nr_runform_th << std::endl;
	std::cout << "NRTH_MRG:		" << odb->nr_merge_th << std::endl;
	std::cout << "NR_RUNS:		" << odb->nr_run << std::endl;
	std::cout << "BLKSIZE(RUNFORM):	" << odb->rf_blksize/1024 << "KB" << std::endl;
	assert(odb->rf_blksize >= 4096);
	std::cout << "BLKSIZE(MERGE):		" << odb->mrg_blksize/1024 << "KB" << std::endl;
	assert(odb->mrg_blksize >= 4096);
	std::cout << "WRITE_UNIT(MERGE):	" << odb->mrg_wrbuf/1024 << "KB" << std::endl;
	std::cout << "TOTAL ENTRIES:		" << odb->total_size/KV_SIZE << std::endl;
}

