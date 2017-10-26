#ifndef HAVE_AFFINITY_H
#define HAVE_AFFINITY_H

int getProcessorCount();
int setSelfAffinitySingleCPU(int cpu);

#endif