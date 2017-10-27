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

#define ELEMENT_MAX 9999999
#define ELEMENT_SIZE 8

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

#define MIN(lhs, rhs) (lhs < rhs ? lhs : rhs)

int workerCount;
uint64_t* presentElements;
char* memIn;
int elementCount;
long mainBegin;

int mapInput(char* fileName, char** memIn, int* elementCount);
int mapOutput(char* fileName, int elementCount, char** memOut, int* fdOut);
void* processPartition(void* workerIndex);
int saveResult(char* fileName, int elementCount);

int main(int argc, char** argv) {
    long end;
    mainBegin = ts();
    workerCount = MIN(getProcessorCount(), 8);
    printf("%04lu workerCount - %u\n", mainBegin - mainBegin, workerCount);

    pthread_t workers[workerCount];

    presentElements = (uint64_t*)calloc(ELEMENT_MAX + 1, sizeof(char*));
    end = ts();
    printf("%04lu calloc presentElements - %lu\n", end - mainBegin, end - mainBegin);

    if (0 != mapInput(argv[1], &memIn, &elementCount)) {
        return 1;
    }

    for (long i = 0; i < workerCount; ++i) {
        pthread_create(&workers[i], NULL, &processPartition, (void*)i);
    }

    char* memOut;
    int fdOut;
    if (0 != mapOutput(argv[2], elementCount, &memOut, &fdOut)) {
        return 1;
    }

    for (int i = 0; i < workerCount; ++i) {
        pthread_join(workers[i], NULL);
    }

    if (0 != saveResult(memOut, elementCount)) {
        return 1;
    }

    end = ts();
    printf("%04lu total - %lu\n", end - mainBegin, end - mainBegin);
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

inline int saveResult(char* memOut, int elementCount) {
    long begin = ts();
    printf("%04lu saveResult begin\n", begin - mainBegin);

    uint64_t* outPtr = (uint64_t*)memOut;
    uint64_t* memOutEnd = (uint64_t*)(memOut + elementCount * ELEMENT_SIZE);
    uint64_t* rangeBegin = presentElements;
    uint64_t* rangeEnd;
    do {
        // first present element
        while (!*rangeBegin) {
            ++rangeBegin;
        }

        rangeEnd = rangeBegin;
        while (*rangeEnd) {
            *outPtr++ = *rangeEnd++;
        }

        rangeBegin = rangeEnd;
    } while (outPtr < memOutEnd);

    long end = ts();
    printf("%04lu saveResult copy - %lu\n", end - mainBegin, end - begin);

    end = ts();
    printf("%04lu saveResult end - %lu\n", end - mainBegin, end - begin);

    return 0;
}

