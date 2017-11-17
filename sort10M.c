#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#include "affinity.h"
#include "clock.h"
#include "mmapHp.h"

#define WORKER_MAX 8

#define ELEMENT_MAX 9999999
#define ELEMENT_SIZE 8

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

#define MIN(lhs, rhs) (lhs < rhs ? lhs : rhs)

int workerCount;
uint64_t* presentElements;
char* memIn;
char* memOut;
int elementCount;
long mainBegin;

int mapInput(char* fileName, char** memIn, int* elementCount);
int mapOutput(char* fileName, int elementCount, char** memOut, int* fdOut);
void* processPartition(void* workerIndex);
int saveResult(char* fileName, int elementCount, int direction);
void* saveResultReverse(void* arg);

int main(int argc, char** argv) {
    long end;
    mainBegin = ts();
    workerCount = MIN(getProcessorCount(), WORKER_MAX);
    printf("%04lu workerCount - %u\n", mainBegin - mainBegin, workerCount);

    pthread_t workers[workerCount];

    if (MAP_FAILED == (presentElements = (uint64_t*)mmapHp(NULL, (ELEMENT_MAX + 1) * sizeof(char*), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS))) {
        perror("allocate presentElements with huge pages, fallback");
    	if (MAP_FAILED == (presentElements = (uint64_t*)mmap(NULL, (ELEMENT_MAX + 1) * sizeof(char*), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
            perror("allocate presentElements");
            return 1;
        }
    }
    end = ts();
    printf("%04lu allocate presentElements - %lu\n", end - mainBegin, end - mainBegin);

    if (0 != mapInput(argv[1], &memIn, &elementCount)) {
        return 1;
    }

    for (long i = 0; i < workerCount; ++i) {
        pthread_create(&workers[i], NULL, &processPartition, (void*)i);
    }

    int fdOut;
    if (0 != mapOutput(argv[2], elementCount, &memOut, &fdOut)) {
        return 1;
    }

    for (int i = 0; i < workerCount; ++i) {
        pthread_join(workers[i], NULL);
    }

    if (workerCount > 1) {
        pthread_create(&workers[1], NULL, &saveResultReverse, (void*)1);
        saveResult(memOut, elementCount / 2, 1);
        pthread_join(workers[1], NULL);
    } else {
        if (0 != saveResult(memOut, elementCount, 1)) {
            return 1;
        }
    }

    end = ts();
    printf("%04lu total - %lu\n", end - mainBegin, end - mainBegin);
    return 0;
}

void* saveResultReverse(void* arg) {
    long workerIndex = (long)arg;
    setSelfAffinitySingleCPU(workerIndex);
    saveResult(memOut + (elementCount / 2) * ELEMENT_SIZE, elementCount / 2, -1);
    return 0;
}

inline int mapInput(char* fileName, char** memIn, int* elementCount) {
    int fdIn;
    struct stat stat;
    long begin = ts();

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
    long end = ts();
    printf("%04lu mapInput end - %lu\n", end - mainBegin, end - begin);
    return 0;
}

inline unsigned int strtolb10(uint64_t str) {
    uint64_t tmp = str - 0x0030303030303030;
    char* tmpStr = (char*)&tmp;
    unsigned int res = 0;

    for (unsigned int i = 0; i < 7; ++i) {
        res = res * 10 + *tmpStr++;
    }
    return res;
}

void* processPartition(void* arg) {
    long begin = ts();
    long workerIndex = (long)arg;
    printf("%04lu processPartition %lu begin\n", begin - mainBegin, workerIndex);

    setSelfAffinitySingleCPU(workerIndex);

    uint64_t* partitionIter = &((uint64_t*)memIn)[workerIndex * (elementCount / workerCount)];
    uint64_t* partitionEnd = &((uint64_t*)memIn)[(workerIndex + 1) * (elementCount / workerCount)];
    for (; partitionIter < partitionEnd; partitionIter++) {
        unsigned int element = strtolb10(*partitionIter);
        presentElements[element] = *partitionIter;
    }

    long end = ts();
    printf("%04lu processPartition %lu end - %lu ms\n", end - mainBegin, workerIndex, end - begin);
    return 0;
}

int mapOutput(char* fileName, int elementCount, char** memOut, int* fdOut) {
    long begin = ts();
    printf("%04lu mapOutput begin\n", begin - mainBegin);

    if (-1 == (*fdOut = open(fileName, O_RDWR | O_CREAT, 0755))) {
        perror("open outfile");
        return -1;
    }

    if (-1 == ftruncate(*fdOut, elementCount * ELEMENT_SIZE)) {
        perror("ftruncate outfile");
        return -1;
    }

    if (MAP_FAILED ==
        (*memOut = (char*)mmap(NULL, elementCount * ELEMENT_SIZE, PROT_WRITE, MAP_SHARED | MAP_POPULATE, *fdOut, 0))) {
        perror("mmap outfile");
        return -1;
    }

    madvise(*memOut, elementCount * ELEMENT_SIZE, MADV_SEQUENTIAL);
    long end = ts();
    printf("%04lu mapOutput end - %lu\n", end - mainBegin, end - begin);

    return 0;
}

inline void swap(uint64_t** lhs, uint64_t** rhs) {
    uint64_t* tmp;
    tmp = *lhs;
    *lhs = *rhs;
    *rhs = tmp;
}

inline int saveResult(char* memOut, int elementCount, int direction) {
    long begin = ts();
    printf("%04lu saveResult dir %i begin\n", begin - mainBegin, direction);

    uint64_t* outPtr = (uint64_t*)memOut;
    uint64_t* memOutEnd = (uint64_t*)(memOut + (elementCount - 1) * ELEMENT_SIZE);
    uint64_t* rangeBegin = presentElements;
    uint64_t* rangeEnd = presentElements + ELEMENT_MAX;
    int copied = 0;

    if (-1 == direction) {
       swap(&outPtr, &memOutEnd);
       swap(&rangeBegin, &rangeEnd);
    }

    do {
        // first present element
        while (!*rangeBegin) {
            rangeBegin += direction;
        }

        rangeEnd = rangeBegin;
        while (*rangeEnd) {
            copied++;
            *outPtr = *rangeEnd;
            outPtr += direction;
            rangeEnd += direction;
        }

        rangeBegin = rangeEnd;
    } while (copied < elementCount);

    long end = ts();
    printf("%04lu saveResult dir %i end - %lu\n", end - mainBegin, direction, end - begin);

    return 0;
}

