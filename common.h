#ifndef __COMMON_H
#define __COMMON_H

#include "opt.h"

#define BILLION     (1000000001ULL)
#define calclock(timevalue, total_time, total_count) do { \
	unsigned long long timedelay, temp, temp_n; \
	struct timespec *myclock = (struct timespec*)timevalue; \
	if(myclock[1].tv_nsec >= myclock[0].tv_nsec){ \
		temp = myclock[1].tv_sec - myclock[0].tv_sec; \
		temp_n = myclock[1].tv_nsec - myclock[0].tv_nsec; \
		timedelay = BILLION * temp + temp_n; \
	} else { \
		temp = myclock[1].tv_sec - myclock[0].tv_sec - 1; \
		temp_n = BILLION + myclock[1].tv_nsec - myclock[0].tv_nsec; \
		timedelay = BILLION * temp + temp_n; \
	} \
	__sync_fetch_and_add(total_time, timedelay); \
	__sync_fetch_and_add(total_count, 1); \
} while(0)

struct TimeStats{
	unsigned long long first_time;
	unsigned long long last_time;
	unsigned long long avg_total;
	unsigned long long avg_read;
	unsigned long long avg_write;
	unsigned long long avg_sort;
};

struct TimeFormat{
	unsigned long long total_t;
	unsigned long long total_c;
	unsigned long long *arrival_t;
	unsigned long long *arrival_c;
	unsigned long long *read_t;
	unsigned long long *read_c;
	unsigned long long *write_t;
	unsigned long long *write_c;
	unsigned long long *sort_t;
	unsigned long long *sort_c;
};


struct Data{
	uint64_t key;
	char value[KV_SIZE-sizeof(uint64_t)];
};

struct id_Data{
	uint32_t run_id;
	struct Data data;
	bool operator>(const id_Data &cmp) const {
		return data.key > cmp.data.key;
	}
};

struct RunDesc{
	int run_id;
	uint64_t valid_entries;	/* entry */
	uint64_t blk_ofs;		/* entry */
	uint64_t rw_size;		/* byte  */
	uint64_t rw_ofs;		/* byte  */
};


extern int do_verify;
extern int do_profile;
extern int do_clear;
extern int64_t ReadData(int, char*, int64_t);
extern int64_t WriteData(int, char*, int64_t);
extern int64_t pReadData(int, char*, int64_t, uint64_t);
extern int64_t pWriteData(int, char*, int64_t, uint64_t);

#endif /* COMMON_H */

