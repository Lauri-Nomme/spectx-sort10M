#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char** argv) {
    int fdIn;
    FILE * fOut;
    struct stat stat;
    char* memIn;
    char* presentElements;
    int workerCount = 1;
    int workerIndex = 0;
    int elementSize = strlen("9999999\n");
    int elementCount;

    presentElements = calloc(10 * 1000 * 1000, 1);

    fdIn = open(argv[1], O_RDONLY);
    fstat(fdIn, &stat);
    elementCount = stat.st_size / elementSize;

    if (MAP_FAILED == (memIn = mmap(NULL, stat.st_size, PROT_READ, MAP_SHARED | MAP_POPULATE, fdIn, 0))) {
        perror("mmap infile");
        return 1;
    }

    madvise(memIn, stat.st_size, POSIX_MADV_SEQUENTIAL);

    for (int i = workerIndex; i < elementCount; i += workerCount) {
        int number = strtol(memIn + elementSize * i, NULL, 10);
        presentElements[number] = 1;
    }

    fOut = fopen(argv[2], "w");
    for (int i = 0; i <= 9999999; ++i) {
        if (presentElements[i]) {
            fprintf(fOut, "%07i\n", i);
        }
    }
    fclose(fOut);
}