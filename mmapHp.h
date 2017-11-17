#ifndef HAVE_MMAP_HP_H
#define HAVE_MMAP_HP_H

#include <stddef.h>

void* mmapHp(void* addr, size_t length, int prot, int flags);
#endif
