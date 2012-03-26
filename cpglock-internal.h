#ifndef _CPGLOCK_INT_H
#define _CPGLOCK_INT_H

#ifdef CPGLOCK_DEBUG
#	define cpgl_debug(format, args...) do { printf(format, ##args); } while (0)
#else
#	define cpgl_debug(format, args...) do { } while (0)
#endif

#ifndef CPG_LOCKD_SOCK
#define CPG_LOCKD_SOCK "/var/run/cpglockd.sk"
#endif

#include <stdint.h>

typedef enum {
	MSG_LOCK   = 1,
	MSG_NAK    = 2,
	MSG_GRANT  = 3,
	MSG_UNLOCK = 4,
	MSG_PURGE  = 5,
	MSG_CONFCHG= 6,
	MSG_JOIN   = 7,
	MSG_DUMP   = 998,
	MSG_HALT   = 999
} cpg_lock_req_t;

/* Mixed architecture not supported yet */
struct __attribute__((packed)) cpg_lock_msg {
	char resource[96];
	int32_t request;
	uint32_t owner_nodeid;
	uint32_t owner_pid;
	uint32_t flags;
	uint32_t lockid;
	uint32_t owner_tid;
	char pad[8];
}; /* 128 */

pid_t _gettid (void);

#define CPG_LOCKD_NAME "cpglockd"

#endif
