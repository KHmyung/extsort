#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>


int64_t
ReadData(int fd, char* buf, int64_t buf_size){
	int64_t size = 0;
	int len = 0;

	while(1){
		if((len = read(fd, &buf[0] + size, buf_size - size)) > 0){
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				continue;
			}
			else return -1;
		}
	}
}

int64_t
WriteData(int fd, char* buf, int64_t buf_size){
	int64_t size = 0;
	int len = 0;

	while(1){
		if((len = write(fd, &buf[0] + size, buf_size - size)) > 0){
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				continue;
			}
			else {
				return -1;
			}
		}
	}
}

int64_t
pReadData(int fd, char* buf, int64_t buf_size, uint64_t ofs){
	int64_t size = 0;
	int len = 0;

	while(1){
		if((len = pread(fd, &buf[0] + size, buf_size - size, ofs)) > 0){
			ofs += len;
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				continue;
			}
			else return -1;
		}
	}
}

int64_t
pWriteData(int fd, char* buf, int64_t buf_size, uint64_t ofs){
	int64_t size = 0;
	int len = 0;

	while(1){
		if((len = pwrite(fd, &buf[0] + size, buf_size - size, ofs)) > 0){
			ofs += len;
			size += len;
			if(size == buf_size){
				return size;
			}
		}
		else if(len == 0){
			return size;
		}
		else{
			if(errno == EINTR){
				continue;
			}
			else return -1;
		}
	}
}
