#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "cpglock-internal.h"

/* Patch from Adam Conrad / Ubuntu: Don't use _syscall macro */

#ifdef __NR_gettid
pid_t _gettid (void)
{
	return syscall(__NR_gettid);
}
#else

#warn "gettid not available -- substituting with pthread_self()"

#include <pthread.h>
pid_t _gettid (void)
{
	return (pid_t)pthread_self();
}
#endif
