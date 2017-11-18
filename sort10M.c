#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
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
unsigned int elementCount;
long mainBegin;

int mapInput(char* fileName, char** memIn, unsigned int* elementCount);
int mapOutput(char* fileName, unsigned int elementCount, char** memOut, int* fdOut);
void* processPartition(void* workerIndex);
int saveResult(char* fileName, unsigned int elementCount, int direction);
void* saveResultReverse(void* arg);

inline unsigned int strtolb10(uint64_t str) {
    uint64_t tmp = str - 0x0030303030303030;
    char* tmpStr = (char*)&tmp;
    unsigned int res = 0;

    for (unsigned int i = 0; i < 7; ++i) {
        res = res * 10 + *tmpStr++;
    }
    return res;
}

inline void ltostrb10(unsigned int l, uint64_t* dest) {
    *dest = 0x0a30303030303030;
    char* str = (char*)dest + 6;

    //while (l) {
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
        *str-- += l % 10;
        l /= 10;
    //}
}

inline size_t presentElementsSize(unsigned int elementCount) {
    return (elementCount + 7) / 8;
}

inline unsigned int elementIndex(unsigned int element) {
    return element / 32;
}

inline uint32_t elementMask(unsigned int element) {
    return (uint32_t)1 << (element % 32); 
}

inline void presentElementsMark(uint64_t* str) {
    unsigned int element = strtolb10(*str);
    ((uint32_t*)presentElements)[elementIndex(element)] |= elementMask(element);
}

inline int presentElementsRetrieve(unsigned int element, uint64_t* dest) {
    //printf("read %i idx %u data %016" PRIx64 " mask %016" PRIx64 "\n", element, elementIndex(element), presentElements[elementIndex(element)], elementMask(element));
    if (!(((uint32_t*)presentElements)[elementIndex(element)] & elementMask(element))) {
        return 0;
    }

    ltostrb10(element, dest);
    return 1;
}

int main(int argc, char** argv) {
    long end;
    mainBegin = ts();
    workerCount = MIN(getProcessorCount(), WORKER_MAX);
    printf("%04lu workerCount - %u\n", mainBegin - mainBegin, workerCount);

    pthread_t workers[workerCount];

    if (MAP_FAILED == (presentElements = (uint64_t*)mmapHp(NULL, presentElementsSize(ELEMENT_MAX + 1), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS))) {
        perror("allocate presentElements with huge pages, fallback");
    	if (MAP_FAILED == (presentElements = (uint64_t*)mmap(NULL, presentElementsSize(ELEMENT_MAX + 1), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
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

inline int mapInput(char* fileName, char** memIn, unsigned int* elementCount) {
    int fdIn;
    struct stat stat;
    long begin = ts();

    if (-1 == (fdIn = open(fileName, O_RDONLY))) {
        perror("open infile");
        return -1;
    }

    fstat(fdIn, &stat);
    *elementCount = stat.st_size / ELEMENT_SIZE;

    if (MAP_FAILED == (*memIn = (char*)mmap(NULL, stat.st_size, PROT_READ, MAP_PRIVATE, fdIn, 0))) {
        perror("mmap infile");
        return -1;
    }

    madvise(*memIn, stat.st_size, MADV_SEQUENTIAL);
    long end = ts();
    printf("%04lu mapInput end - %lu\n", end - mainBegin, end - begin);
    return 0;
}


void* processPartition(void* arg) {
    long begin = ts();
    long workerIndex = (long)arg;
    printf("%04lu processPartition %lu begin\n", begin - mainBegin, workerIndex);

    setSelfAffinitySingleCPU(workerIndex);

    uint64_t* partitionIter = &((uint64_t*)memIn)[workerIndex * (elementCount / workerCount)];
    uint64_t* partitionEnd = &((uint64_t*)memIn)[(workerIndex + 1) * (elementCount / workerCount)];
    for (; partitionIter < partitionEnd; partitionIter++) {
        presentElementsMark(partitionIter);
    }

    long end = ts();
    printf("%04lu processPartition %lu end - %lu ms\n", end - mainBegin, workerIndex, end - begin);
    return 0;
}

int mapOutput(char* fileName, unsigned int elementCount, char** memOut, int* fdOut) {
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
        (*memOut = (char*)mmap(NULL, elementCount * ELEMENT_SIZE, PROT_WRITE, MAP_SHARED, *fdOut, 0))) {
        perror("mmap outfile");
        return -1;
    }

    madvise(*memOut, elementCount * ELEMENT_SIZE, MADV_SEQUENTIAL);
    long end = ts();
    printf("%04lu mapOutput end - %lu\n", end - mainBegin, end - begin);

    return 0;
}

inline int saveResult(char* memOut, unsigned int elementCount, int direction) {
    long begin = ts();
    printf("%04lu saveResult dir %i begin\n", begin - mainBegin, direction);

    uint64_t* outPtr;
    unsigned int elementIter;
    unsigned int copied = 0;

    if (1 == direction) {
        outPtr = (uint64_t*)memOut;
        elementIter = 0;
    } else {
        outPtr = (uint64_t*)(memOut + (elementCount - 1) * ELEMENT_SIZE);
        elementIter = ELEMENT_MAX;
    }

    do {
        if (presentElementsRetrieve(elementIter, outPtr)) {
            outPtr += direction;
            copied++;
        }
        elementIter += direction;
    } while (copied < elementCount);

    long end = ts();
    printf("%04lu saveResult dir %i end - %lu\n", end - mainBegin, direction, end - begin);

    return 0;
}

