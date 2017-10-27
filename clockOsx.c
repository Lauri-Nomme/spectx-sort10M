#include <time.h>
#if defined(__MACH__) && !defined(CLOCK_REALTIME)
#include <sys/time.h>
int clock_gettime(int clk_id, struct timespec* t) {
    struct timeval now;
    int rv = gettimeofday(&now, NULL);
    if (rv) return rv;
    t->tv_sec  = now.tv_sec;
    t->tv_nsec = now.tv_usec * 1000;
    return 0;
}
#endif

long ts() {
    struct timespec tv;
    clock_gettime(0, &tv);
    return (tv.tv_sec * 1000) + tv.tv_nsec / 1000000;
}
