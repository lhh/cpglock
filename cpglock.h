#ifndef _CPGLOCK_H
#define _CPGLOCK_H

typedef enum {
	LOCK_FREE    = 0,
	LOCK_PENDING = 1,
	LOCK_HELD    = 2
} lock_state_t;

typedef enum {
	FL_TRY = 0x1
} lock_flag_t;

struct cpg_lock {
	char resource[96];
	int local_fd;
	int local_id;
	lock_state_t state;
	int owner_nodeid;
	int owner_pid;
	int owner_tid;
};

typedef void * cpg_lock_handle_t;

int cpg_lock_init(cpg_lock_handle_t *h);

/* 0 if successful, -1 if error */
int cpg_lock(cpg_lock_handle_t h, const char *resource,
	     lock_flag_t flags, struct cpg_lock *lock);

int cpg_unlock(cpg_lock_handle_t h,
	       struct cpg_lock *lock);

/* Warning: drops all locks with this client */
int cpg_lock_fin(cpg_lock_handle_t h);

int cpg_lock_dump(FILE *fp);

#endif
