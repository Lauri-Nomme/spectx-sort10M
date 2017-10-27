#if defined(__MACH__) && !defined(CLOCK_REALTIME)
#define CLOCK_REALTIME 0
int clock_gettime(int /*clk_id*/, struct timespec* t);
#endif
