#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <sched.h>

int getProcessorCount() {
    return sysconf(_SC_NPROCESSORS_ONLN);
}

int setSelfAffinitySingleCPU(int cpu) {
#ifdef cpu_set_t
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
