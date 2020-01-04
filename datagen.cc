#include "datagen.h"

static struct
Data* alloc_buf(uint64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
}

static void
randstring(size_t length, char *buf, unsigned int seed) {
	static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";
	if (buf) {
		int l = (int) (sizeof(charset) -1);
		int key;
		for (int n = 0;n < length;n++) {
			key = rand_r(&seed) % l;
			buf[n] = charset[key];
		}
		buf[length] = '\0';
	}
}

static void*
t_DataGeneration(void* data){

	struct DatagenArgs args = *(struct DatagenArgs*)data;
	int fd_input = args.fd_input;
	int th_idx = args.th_idx;
	int nr_thread = args.nr_thread;
	uint64_t data_size = args.data_size;
	uint64_t mem_size = args.mem_size;
	uint64_t writeofs = args.writeofs;
	unsigned int seed = (unsigned int)th_idx;

	Data *genbuf = alloc_buf(mem_size);

	uint64_t nbyte_buffer = 0;
	uint64_t nbyte_written = 0;
	uint64_t offset = 0;

	for(size_t i = 0; i < data_size/KV_SIZE; i++){

		randstring(sizeof(genbuf[offset].value), genbuf[offset].value, (unsigned int)seed);
		genbuf[offset].key = rand_r((unsigned int*)&seed) % MAXKEY;

		offset++;
		nbyte_buffer += KV_SIZE;

		if(nbyte_buffer == mem_size){
			int64_t tmp_write = pwrite(fd_input, (char*)&genbuf[0], mem_size, writeofs);
			assert(tmp_write == mem_size);
			writeofs += tmp_write;

			nbyte_written += nbyte_buffer;
			nbyte_buffer = 0;
			offset = 0;
		}
	}

	free(genbuf);

	return (void*)1;
}

void
DataGeneration(void* data){
	struct opt_t odb = *(struct opt_t *)data;

	int fd_input = open( odb.inpath.c_str(), O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
	assert(fd_input > 0);

	pthread_t p_thread[odb.nr_datagen_th];
	int thread_id[odb.nr_datagen_th];
	struct DatagenArgs datagen_args[odb.nr_datagen_th];


	for(int th = 0; th < odb.nr_datagen_th; th++){
		datagen_args[th].fd_input = fd_input;
		datagen_args[th].th_idx = th;
		datagen_args[th].nr_thread = odb.nr_datagen_th;
		datagen_args[th].data_size = odb.total_size/odb.nr_datagen_th;
		datagen_args[th].mem_size = odb.mem_size/odb.nr_datagen_th;
		datagen_args[th].writeofs = th*(odb.total_size/odb.nr_datagen_th);

		thread_id[th] = pthread_create(&p_thread[th], NULL, t_DataGeneration, (void*)&datagen_args[th]);
	}

	int is_ok = 0;
	int res = 0;
	for(int th = 0; th < odb.nr_datagen_th; th++){
		pthread_join(p_thread[th], (void **)&is_ok);
		if(is_ok != 1){
			res = -1;
		}
	}

	close(fd_input);
}

