#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

int workerCount = 4;
char* presentElements;
char* memIn;
int elementCount;
int elementSize = 8;

void* processPartition(void* workerIndex);
void saveResult(char* fileName);

int main(int argc, char** argv) {
    int fdIn;
    struct stat stat;
    pthread_t workers[workerCount];

    presentElements = calloc(10 * 1000 * 1000, 1);

    fdIn = open(argv[1], O_RDONLY);
    fstat(fdIn, &stat);
    elementCount = stat.st_size / elementSize;

    if (MAP_FAILED == (memIn = mmap(NULL, stat.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fdIn, 0))) {
        perror("mmap infile");
        return 1;
    }

    madvise(memIn, stat.st_size, POSIX_MADV_SEQUENTIAL);

    for (long i = 0; i < workerCount; ++i) {
        pthread_create(&workers[i], NULL, &processPartition, (void*)i);
    }

    for (int i = 0; i < workerCount; ++i) {
        pthread_join(workers[i], NULL);
    }

    saveResult(argv[2]);

    return 0;
}

void* processPartition(void* workerIndex) {
    for (long i = (long)workerIndex; i < elementCount; i += workerCount) {
        int number = strtol(memIn + elementSize * i, NULL, 10);
        presentElements[number] = 1;
    }
}

void saveResult(char* fileName) {
    FILE* fOut = fopen(fileName, "w");
    for (int i = 0; i <= 9999999; ++i) {
        if (presentElements[i]) {
            fprintf(fOut, "%07i\n", i);
        }
    }

    fclose(fOut);
}