#ifndef _PROFILE_H_
#define _PROFILE_H_

//function level profiling
#define BILLION     (1000000001ULL)
//#define calclock(timevalue, total_time, total_count, delay_time) do { 
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

//Global function time/count variables
unsigned long long total_time, total_count;
int step_cnt;

unsigned long long runformation_time, runformation_count;
unsigned long long reduce_time, reduce_count;
unsigned long long merge_time, merge_count;
unsigned long long runform_arrival_time[256], runform_arrival_count[256];
unsigned long long runform_read_time[256], runform_read_count[256];
unsigned long long runform_write_time[256], runform_write_count[256];
unsigned long long runform_sort_time[256], runform_sort_count[256];
unsigned long long merge_arrival_time[256], merge_arrival_count[256];
unsigned long long merge_read_time[256], merge_read_count[256];
unsigned long long merge_write_time[256], merge_write_count[256];
unsigned long long merge_sort_time[256], merge_sort_count[256];
#endif
