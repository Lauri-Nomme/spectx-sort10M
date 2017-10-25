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

#define ELEMENT_MAX 9999999
#define ELEMENT_SIZE 8

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

int workerCount = 4;
char** presentElements;
char* memIn;
int elementCount;

int mapInput(char* fileName, char** memIn, int* elementCount);
void* processPartition(void* workerIndex);
int saveResult(char* fileName, int elementCount);

int main(int argc, char** argv) {
    pthread_t workers[workerCount];

    presentElements = (char**)calloc(ELEMENT_MAX + 1, sizeof(char*));

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

int mapInput(char* fileName, char** memIn, int* elementCount) {
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

void* processPartition(void* workerIndex) {
    for (long i = (long)workerIndex; i < elementCount; i += workerCount) {
        int number = strtol(memIn + ELEMENT_SIZE * i, NULL, 10);
        presentElements[number] = memIn + ELEMENT_SIZE * i;
    }

    return 0;
}

int saveResult(char* fileName, int elementCount) {
    int fdOut;
    char* memOut;

    if (-1 == (fdOut = open(fileName, O_RDWR | O_CREAT | O_TRUNC, 0755))) {
        perror("open outfile");
        return -1;
    }

    if (-1 == lseek(fdOut, elementCount * ELEMENT_SIZE - 1, SEEK_SET)) {
        perror("seek outfile");
        return -1;
    }

    if (-1 == write(fdOut, "", 1)) {
        perror("write outfile");
        return -1;
    }

    if (MAP_FAILED == (memOut = (char*)mmap(NULL, elementCount * ELEMENT_SIZE, PROT_WRITE, MAP_SHARED | MAP_POPULATE, fdOut, 0))) {
        perror("mmap outfile");
        return -1;
    }

    madvise(memOut, elementCount * ELEMENT_SIZE, MADV_SEQUENTIAL);

    char* outPtr = memOut;
    for (int i = 0; i <= ELEMENT_MAX; ++i) {
        if (presentElements[i]) {
            *(uint64_t*)outPtr = *(uint64_t*)presentElements[i];
            outPtr += ELEMENT_SIZE;
        }
    }

    munmap(memOut, elementCount * ELEMENT_SIZE);
    close(fdOut);

    return 0;
}
