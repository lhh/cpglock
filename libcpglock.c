#include <stdlib.h>
#include <sys/socket.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#include "cpglock.h"
#include "cpglock-internal.h"
#include "list.h"
#include "sock.h"


struct pending_node {
	list_head();
	struct cpg_lock_msg m;
};


struct cpg_lock_handle {
	pthread_mutex_t mutex;
	struct pending_node *pending;
	int fd;
	int pid;
	int seq;
};


static int
check_pending(struct pending_node **l,
	      struct cpg_lock_msg *exp,
	      struct cpg_lock_msg *ret)
{
	struct pending_node *p;
	int x;

	list_for(l, p, x) {
		if (!strcmp(exp->resource, p->m.resource) &&
		    exp->owner_tid == p->m.owner_tid) {
			list_remove(l, p);
			memcpy(ret, &p->m, sizeof(*ret));
			free(p);
			return 0;
		}
	}

	return 1;
}


static void
add_pending(struct pending_node **l,
	    struct cpg_lock_msg *m)
{
	struct pending_node *p = do_alloc(sizeof(*p));

	memcpy(&p->m, m, sizeof(p->m));
	list_insert(l, p);
}


/* Not thread safe */
int
cpg_lock_init(void **handle)
{
	struct cpg_lock_handle *h;
	int esv;

	h = do_alloc(sizeof (*h));
	if (!h)
		return -1;

	h->fd = sock_connect(CPG_LOCKD_SOCK, 3);
	if (h->fd < 0) {
		esv = errno;
		free(h);
		errno = esv;
		return -1;
	}

	h->pid = getpid();
	pthread_mutex_init(&h->mutex, NULL);

	*handle = (void *)h;
	return 0;
}


int
cpg_lock(void *handle, const char *resource, lock_flag_t flags, struct cpg_lock *lock)
{
	struct cpg_lock_handle *h = handle;
	struct cpg_lock_msg l, r;
	struct timeval tv;
	int ret = -1;

	if (!h) {
		errno = EINVAL;
		goto out;
	}

	pthread_mutex_lock(&h->mutex);

	if (h->pid != (int)getpid()) {
		errno = EBADF;
		goto out;
	}

	if (strlen(resource) > sizeof(l.resource)-1) {
		errno = ENAMETOOLONG;
		goto out;
	}

	memset(&l, 0, sizeof(l));
	memset(&r, 0, sizeof(r));
	strncpy(l.resource, resource, sizeof(l.resource));
	strncpy(lock->resource, resource, sizeof(lock->resource));
	l.owner_pid = h->pid;
	++h->seq;
	l.owner_tid = h->seq;
	l.request = MSG_LOCK;
	l.flags = (uint32_t)flags;
	
	if (write_retry(h->fd, &l, sizeof(l), 0) < 0)
		goto out;

	/* Thread concurrency: in case multiple threads wake up
	   from select, peek at the message to see if it's ours */
	do {
		if (check_pending(&h->pending, &l, &r) == 0)
			break;

		tv.tv_sec = 0;
		tv.tv_usec = random() & 16383;

		if (read_retry(h->fd, &r, sizeof(r), &tv) < 0) {
			if (errno == ETIMEDOUT) {
				pthread_mutex_unlock(&h->mutex);
				usleep(random() & 16383);
				pthread_mutex_lock(&h->mutex);
				continue;
			}
			goto out;
		}

		if (strcmp(r.resource, l.resource)) {
			printf("NOTE: msg for wrong lock want: %s got: %s\n", l.resource, r.resource);
			add_pending(&h->pending, &r);
			pthread_mutex_unlock(&h->mutex);
			usleep(random() & 16383);
			pthread_mutex_lock(&h->mutex);
			continue;
		}
		if (r.owner_tid != l.owner_tid) {
			printf("NOTE: msg for wrong seq want: %d got: %d\n", l.owner_tid, r.owner_tid );
			add_pending(&h->pending, &r);
			pthread_mutex_unlock(&h->mutex);
			usleep(random() & 16383);
			pthread_mutex_lock(&h->mutex);
			continue;
		}
		break;
	} while (1);
	/* locked */

	if (r.owner_nodeid == 0)
		goto out;

	if (r.request == MSG_NAK) {
		errno = EAGAIN;
		return -1;
	}

	if (r.request != MSG_GRANT) {
		//ret = -1;
		goto out;
	}

	lock->state = LOCK_HELD;
	lock->owner_nodeid = r.owner_nodeid;
	lock->owner_pid = h->pid; /* XXX */
	lock->local_id = r.lockid;

	ret = 0;

out:
	pthread_mutex_unlock(&h->mutex);
	return ret;
}


int
cpg_unlock(void *handle, struct cpg_lock *lock)
{
	struct cpg_lock_handle *h = handle;
	struct cpg_lock_msg l;
	int ret = -1;

	if (!h) {
		errno = EINVAL;
		goto out;
	}

	/* Only block on lock requests, not unlock */
	if (h->pid != (int)getpid()) {
		errno = EBADF;
		goto out;
	}

	if (lock->state != LOCK_HELD) {
		errno = EINVAL;
		goto out;
	}

	if (!lock->local_id) {
		errno = EINVAL;
		goto out;
	}

	strncpy(l.resource, lock->resource, sizeof(l.resource));
	l.request = MSG_UNLOCK;
	l.owner_nodeid = lock->owner_nodeid;
	l.owner_pid = lock->owner_pid;
	l.lockid = lock->local_id;
	//l.owner_tid = _gettid();
	l.owner_tid = 0;
	lock->state = LOCK_FREE;
	
	ret = write_retry(h->fd, &l, sizeof(l), 0);
out:
	return ret;
}


int
cpg_lock_dump(FILE *fp)
{
	struct cpg_lock_msg l;
	int fd;
	char c;

	fd = sock_connect(CPG_LOCKD_SOCK, 3);
	if (fd < 0)
		return -1;

	memset(&l, 0, sizeof(l));
	l.request = MSG_DUMP;
	
	if (write_retry(fd, &l, sizeof(l), 0) < 0)
		return -1;

	while (read_retry(fd, &c, 1, 0) == 1)
		fprintf(fp, "%c", c);
	
	close(fd);
	return 0;
}



/* Not thread safe */
int
cpg_lock_fin(void *handle)
{
	struct cpg_lock_handle *h = handle;

	if (!h) {
		errno = EINVAL;
		return -1;
	}

	pthread_mutex_lock(&h->mutex);

	if (h->pid != (int)getpid()) {
		errno = EBADF;
		return -1;
	}

	close(h->fd);

	pthread_mutex_destroy(&h->mutex);
	free(h);
	return 0;
}

