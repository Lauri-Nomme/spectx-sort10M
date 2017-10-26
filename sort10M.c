#define _GNU_SOURCE

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>

#define ELEMENT_MAX 9999999
#define ELEMENT_SIZE 8

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

int workerCount;
uint64_t* presentElements;
char* memIn;
int elementCount;

int mapInput(char* fileName, char** memIn, int* elementCount);
void* processPartition(void* workerIndex);
int saveResult(char* fileName, int elementCount);
int getProcessorCount();
int setSelfAffinitySingleCPU(int cpu);

int main(int argc, char** argv) {
    workerCount = getProcessorCount();
    printf("workerCount - %d\n", workerCount);
    
    pthread_t workers[workerCount];

    presentElements = (uint64_t*)calloc(ELEMENT_MAX + 1, sizeof(char*));

    if (0 != mapInput(argv[1], &memIn, &elementCount)) {
        return 1;
    }

    for (long i = 0; i < workerCount; ++i) {
        pthread_create(&workers[i], NULL, &processPartition, (void*)i);
    }

    for (int i = 0; i < workerCount; ++i) {
        pthread_join(workers[i], NULL);
    }

    if (0 != saveResult(argv[2], elementCount)) {
        return 1;
    }

    return 0;
}

inline int mapInput(char* fileName, char** memIn, int* elementCount) {
    int fdIn;
    struct stat stat;

    if (-1 == (fdIn = open(fileName, O_RDONLY))) {
        perror("open infile");
        return -1;
    }

    fstat(fdIn, &stat);
    *elementCount = stat.st_size / ELEMENT_SIZE;

    if (MAP_FAILED == (*memIn = (char*)mmap(NULL, stat.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fdIn, 0))) {
        perror("mmap infile");
        return -1;
    }

    madvise(*memIn, stat.st_size, MADV_SEQUENTIAL);
    return 0;
}

inline unsigned int strtolb10(char* str) {
    uint64_t tmp = *(uint64_t*)str - 0x0030303030303030;
    char* tmpStr = (char*)&tmp;
    //printf("%02X %02X %02X %02X %02X %02X %02X %02X \n", tmpStr[0], tmpStr[1], tmpStr[2], tmpStr[3], tmpStr[4], tmpStr[5], tmpStr[6], tmpStr[7]);
    unsigned int res = 0;
    /*res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;
    res *= 10;
    tmp >>= 8;
    res += tmp & 0xFF;*/
    for (unsigned int i = 0; i < 7; ++i) {
        res = res * 10 + *tmpStr++;
    }
    return res;
    //return (tmpStr[6] + 10 * (tmpStr[5] + 10 * (tmpStr[4] + 10 * (tmpStr[3] + 10 * (tmpStr[2] + 10 * (tmpStr[1] + 10 * tmpStr[0]))))));
    //return atoi(str);
}

void* processPartition(void* arg) {
    long workerIndex = (long)arg;

	setSelfAffinitySingleCPU(workerIndex);

    for (long i = workerIndex; i < elementCount; i += workerCount) {
        char* s = memIn + ELEMENT_SIZE * i;
        unsigned int element = strtolb10(s);
        presentElements[element] = *(uint64_t*)s;
    }

    return 0;
}

inline int saveResult(char* fileName, int elementCount) {
    int fdOut;
    char* memOut;

    if (-1 == (fdOut = open(fileName, O_RDWR | O_CREAT | O_TRUNC, 0755))) {
        perror("open outfile");
        return -1;
    }

    if (-1 == ftruncate(fdOut, elementCount * ELEMENT_SIZE)) {
        perror("ftruncate outfile");
        return -1;
    }

    if (MAP_FAILED ==
        (memOut = (char*)mmap(NULL, elementCount * ELEMENT_SIZE, PROT_WRITE, MAP_SHARED | MAP_POPULATE, fdOut, 0))) {
        perror("mmap outfile");
        return -1;
    }

    madvise(memOut, elementCount * ELEMENT_SIZE, MADV_SEQUENTIAL);

    char* outPtr = memOut;
	uint64_t* rangeBegin = presentElements;
	uint64_t* rangeEnd; 
	do {
		// first present element
		while (!*rangeBegin) {
			++rangeBegin;
		}

		 // first missing element
		rangeEnd = rangeBegin;
		while (*rangeEnd) {
			++rangeEnd;
		}

		memcpy(outPtr, rangeBegin, (rangeEnd - rangeBegin) * ELEMENT_SIZE);
		outPtr += (rangeEnd - rangeBegin) * ELEMENT_SIZE;
		rangeBegin = rangeEnd;
	} while (rangeBegin < &presentElements[ELEMENT_MAX] && rangeEnd < &presentElements[ELEMENT_MAX]);

    /*for (int i = 0; i <= ELEMENT_MAX; ++i) {
        if (presentElements[i]) {
            *(uint64_t *) outPtr = *(uint64_t * )presentElements[i];
            outPtr += ELEMENT_SIZE;
        }
    }*/

    munmap(memOut, elementCount * ELEMENT_SIZE);
    close(fdOut);

    return 0;
}

int getProcessorCount() {
	#ifdef _SC_NPROCESSORS_ONLN
	return sysconf(_SC_NPROCESSORS_ONLN);
	#else
	return 4;
	#endif
}

int setSelfAffinitySingleCPU(int cpu) {
	#ifdef sched_setaffinity
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu, &cpuset);
	if (-1 == sched_setaffinity(0, sizeof(cpu_set_t), &cpuset)) {
	perror("sched_setaffinity");
		return -1;
	}
	#endif

	return 0;
}
