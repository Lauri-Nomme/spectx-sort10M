#include <time.h>
long ts() {
    struct timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    return (tv.tv_sec * 1000) + tv.tv_nsec / 1000000;
}
