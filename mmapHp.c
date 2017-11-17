#include <sys/mman.h>
#ifdef __APPLE__
#include <mach/vm_statistics.h>
#endif
#include "mmapHp.h"

void* mmapHp(void* addr, size_t length, int prot, int flags) {
	#ifdef MAP_HUGETLB
	return mmap(addr, length, prot, flags | MAP_HUGETLB, -1, 0);
	#endif
	#ifdef VM_FLAGS_SUPERPAGE_SIZE_ANY
	return mmap(addr, length, prot, flags, VM_FLAGS_SUPERPAGE_SIZE_ANY, 0);
	#endif
	return (void*)-1;
}
