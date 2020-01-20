#include "datagen.h"

#define MIN(min, key) (min>key?key:min)
#define MAX(max, key) (max<key?key:max)

static struct
Data* alloc_buf(uint64_t size){
	void *mem;

	posix_memalign(&mem, 4096, size);
	Data *tmp = new (mem) Data;

	return tmp;
}

static void
randstring(size_t length, char *buf, unsigned int seed)
{
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
	int th_id = args.th_id;
	int nr_thread = args.nr_thread;
	uint64_t data_size = args.data_size;
	uint64_t mem_size = args.mem_size;
	uint64_t min_key = MAXKEY;
	uint64_t max_key = 0;
	unsigned int seed = (unsigned int)th_id;

	Data *genbuf = alloc_buf(mem_size);

	uint64_t nbyte_buffer = 0;
	uint64_t nbyte_written = 0;
	uint64_t offset = 0;

	for(size_t i = 0; i < data_size/KV_SIZE; i++){

		randstring(sizeof(genbuf[offset].value), genbuf[offset].value, (unsigned int)seed);
		genbuf[offset].key = rand_r((unsigned int*)&seed) % MAXKEY;

		if(do_verify){
			min_key = MIN(min_key, genbuf[offset].key);
			max_key = MAX(max_key, genbuf[offset].key);
		}

		offset++;
		nbyte_buffer += KV_SIZE;

		if(nbyte_buffer == mem_size){
			int64_t write_byte = WriteData(fd_input, (char*)&genbuf[0], mem_size);
			assert(write_byte == mem_size);

			nbyte_written += write_byte;
			nbyte_buffer = 0;
			offset = 0;
		}
	}
	if(do_verify){
		std::cout << "[" << th_id << "] MIN_KEY: " << min_key << std::endl;
		std::cout << "[" << th_id << "] MAX_KEY: " << max_key << std::endl;
	}

	free(genbuf);

	return (void*)1;
}

void
DataGeneration(void* data){
	struct opt_t odb = *(struct opt_t *)data;

	int nr_file = odb.nr_datagen_th;
	int fd_input[nr_file];

	pthread_t p_thread[odb.nr_datagen_th];
	int thread_id[odb.nr_datagen_th];
	struct DatagenArgs datagen_args[odb.nr_datagen_th];

	for(int i = 0; i < odb.nr_datagen_th; i++){

		fd_input[i] = open( odb.d_inpath[i].c_str(),
				O_DIRECT | O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE, 0644);
		assert(fd_input[i] > 0);

		datagen_args[i].fd_input = fd_input[i];
		datagen_args[i].th_id = i;
		datagen_args[i].nr_thread = odb.nr_datagen_th;
		datagen_args[i].data_size = odb.total_size/odb.nr_datagen_th;
		datagen_args[i].mem_size = odb.mem_size/odb.nr_datagen_th;
		datagen_args[i].writeofs = 0;

		thread_id[i] = pthread_create(&p_thread[i],
				NULL, t_DataGeneration, (void*)&datagen_args[i]);
	}


	int is_ok = 0;
	for(int th = 0; th < odb.nr_datagen_th; th++){
		pthread_join(p_thread[th], (void **)&is_ok);
		assert(is_ok == 1);
	}

	for(int file = 0; file < nr_file; file++){
		close(fd_input[file]);
	}
}

