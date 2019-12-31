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

struct time_profile{
	unsigned long long total_time;
	unsigned long long total_count;
	unsigned long long merge_time;
	unsigned long long merge_count;
	unsigned long long *merge_arrival_time; 
	unsigned long long *merge_arrival_count; 
	unsigned long long *merge_read_time; 
	unsigned long long *merge_read_count; 
	unsigned long long *merge_write_time; 
	unsigned long long *merge_write_count; 
	unsigned long long *merge_sort_time; 
	unsigned long long *merge_sort_count; 
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



#endif /* COMMON_H */
