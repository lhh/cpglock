#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <malloc.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sys/uio.h>

#include <corosync/cpg.h>

#include <ccs.h>
#include <libcman.h>
#include <libfenced.h>

#include "daemon_init.h"
#include "sock.h"
#include "cpglock.h"
#include "cpglock-internal.h"
#include "list.h"

struct request_node {
	list_head();
	struct cpg_lock l;
};

struct lock_node {
	list_head();
	struct cpg_lock l;
};

struct pending_fence_node {
	list_head();
	int nodeid;
	uint64_t fail_time;
};

struct client_node {
	list_head();
	int fd;
	int pid;
};

struct member_node {
	list_head();
	int nodeid;
	uint32_t pid;
};

struct msg_node {
	list_head();
	struct cpg_lock_msg m;
};

/* Local vars  */
static cpg_handle_t cpg;
static uint32_t my_node_id = 0;
static struct pending_fence_node *pending_fencing = NULL;
static struct request_node *requests = NULL;
static struct lock_node *locks = NULL;
static struct client_node *clients = NULL;
static struct member_node *group_members = NULL;
static struct msg_node *messages = NULL;
static int total_members = 0;
static int local_lockid = 0;
static int message_count = 0;
static int joined = 0;
static int cluster_quorate = 0;
static int shutdown_requested = 0;

static cman_node_t cman_nodes[CPG_MEMBERS_MAX];
static int cman_node_count;

static cman_node_t old_cman_nodes[CPG_MEMBERS_MAX];
static int old_cman_node_count;

static int
cman_nodes_lost(cman_node_t *old_nodes,
				size_t old_node_len,
				cman_node_t *new_nodes,
				size_t new_node_len,
				cman_node_t *lost_nodes,
				size_t lost_nodes_size)
{
	int i;
	size_t lost_nodes_len = 0;

	if (lost_nodes_size < old_node_len) {
		cpgl_debug("Lost nodes array smaller than old nodes array");
		return -1;
	}

	for (i = 0 ; i < old_node_len ; i++) {
		if (old_nodes[i].cn_member) {
			int present_in_new_list = 0;
			int j;

			for (j = 0 ; j < new_node_len ; j++) {
				if (new_nodes[j].cn_nodeid == old_nodes[i].cn_nodeid) {
					present_in_new_list = 1;
					if (!new_nodes[j].cn_member) {
						memcpy(&lost_nodes[lost_nodes_len++],
							&old_nodes[i], sizeof(*lost_nodes));
						break;
					}
				}
			}

			if (!present_in_new_list) {
				memcpy(&lost_nodes[lost_nodes_len++],
					&old_nodes[i], sizeof(*lost_nodes));
			}
		}
	}

	return lost_nodes_len;
}

static int
node_has_fencing(int nodeid)
{
	int ccs_desc;
	char *val = NULL;
	char buf[1024];
	int ret = 1;

	ccs_desc = ccs_connect();
	if (ccs_desc < 0) {
		cpgl_debug("Unable to connect to ccsd\n");
		/* Assume node has fencing */
		return 1;
	}

	snprintf(buf, sizeof(buf),
		 "/cluster/clusternodes/clusternode[@nodeid=\"%d\"]"
		 "/fence/method/device/@name", nodeid);

	if (ccs_get(ccs_desc, buf, &val) != 0)
		ret = 0;
	if (val)
		free(val);
	ccs_disconnect(ccs_desc);
	return ret;
}

static int
fence_domain_joined(void) {
	int ret;
	struct fenced_node fn;

	ret = fenced_node_info(FENCED_NODEID_US, &fn);
	if (ret < 0) {
		cpgl_debug("Unable to determine fence domain membership\n");
		return 0;
	}

	return fn.member;
}

static void
cman_callback(cman_handle_t ch, void *privdata, int reason, int arg)
{
	if (reason == CMAN_REASON_STATECHANGE) {
		int i;
		int ret;
		int nl;
		time_t cur_time = time(NULL);
		size_t nodes_elem = sizeof(cman_nodes) / sizeof(cman_nodes[0]);
		cman_node_t lost_nodes[nodes_elem];

		cluster_quorate = arg;

		memcpy(&old_cman_nodes, &cman_nodes, sizeof(old_cman_nodes));
		old_cman_node_count = cman_node_count;

		memset(&cman_nodes, 0, sizeof(cman_nodes));
		cman_node_count = 0;

		ret = cman_get_nodes(ch, nodes_elem, &cman_node_count, cman_nodes);
		if (ret < 0) {
			cpgl_debug("Unable to get cman nodes list\n");
			return;
		}

		nl = cman_nodes_lost(old_cman_nodes, old_cman_node_count,
				cman_nodes, cman_node_count, lost_nodes, nodes_elem);
		if (nl < 0) {
			cpgl_debug("Unable to get list of lost nodes");
			return;
		}

		for (i = 0 ; i < nl ; i++) {
			int cur_nodeid = lost_nodes[i].cn_nodeid;

			if (node_has_fencing(cur_nodeid)) {
				struct pending_fence_node *pf;

				cpgl_debug("Lost node %d\n", cur_nodeid);
				pf = do_alloc(sizeof(*pf));
				pf->nodeid = cur_nodeid;
				pf->fail_time = cur_time;
				list_append(&pending_fencing, pf);
			} else {
				cpgl_debug("Lost node %d but fencing not configured\n",
					cur_nodeid);
			}
		}
	} else if (reason == CMAN_REASON_TRY_SHUTDOWN) {
		shutdown_requested = 1;
	}
}

static void
wait_for_fencing_join(int nodeid)
{
	if (node_has_fencing(nodeid)) {
		cpgl_debug("Waiting for fence domain join operation to complete\n");
		while (!fence_domain_joined())
			sleep(1);
		cpgl_debug("Fence domain joined\n");
	} else
		cpgl_debug("No fencing is configured, not waiting\n");
}

static void
wait_for_quorum_formation(cman_handle_t ch) {
	cpgl_debug("Waiting for quorum to form\n");
	while (!(cluster_quorate = cman_is_quorate(ch)))
		sleep(1);
	cpgl_debug("Quorum formed\n");
}

static int
cman_connect(cman_handle_t *ch)
{
	assert(ch != NULL);

	*ch = cman_init(NULL);
	if (!*ch) {
		cpgl_debug("Waiting for CMAN to start\n");
		while (!(*ch = cman_init(NULL)))
			sleep(1);
		cpgl_debug("CMAN started\n");
	}

	return 0;
}

static const char *
ls2str(int x)
{
	switch(x){
	case LOCK_FREE: return "FREE";
	case LOCK_HELD: return "HELD";
	case LOCK_PENDING: return "PENDING";
	}
	return "unknown";
}


static const char *
rq2str(int x)
{
	switch(x){
	case MSG_LOCK: return "LOCK";
	case MSG_UNLOCK: return "UNLOCK";
	case MSG_GRANT: return "GRANT";
	case MSG_NAK: return "NAK";
	case MSG_PURGE: return "PURGE";
	case MSG_CONFCHG: return "CONFCHG";
	case MSG_JOIN: return "JOIN";
	case MSG_HALT: return "HALT";
	}
	return "unknown";
}


static void 
dump_state(FILE *fp)
{
	struct request_node *r = NULL;
	struct lock_node *l = NULL;
	struct client_node *c = NULL;
	struct member_node *m = NULL;
	struct msg_node *s = NULL;
	int x;

	fprintf(fp, "cpglockd state\n");
	fprintf(fp, "======== =====\n");

	fprintf(fp, "Node ID: %d\n", my_node_id);
	
	if (group_members) {
		fprintf(fp, "Participants:");
		list_for(&group_members, m, x) {
			fprintf(fp, " %d.%u", m->nodeid, m->pid);
		}
		fprintf(fp, "\n");
	}
	if (clients) {
		fprintf(fp, "Clients:");
		list_for(&clients, c, x) {
			fprintf(fp, " %d.%d", c->pid, c->fd );
		}
		fprintf(fp, "\n");
	}
	fprintf(fp, "\n");

	if (locks) {
		fprintf(fp, "Locks\n");
		fprintf(fp, "=====\n");
		list_for(&locks, l, x) {
			fprintf(fp, "  %s: %s", l->l.resource, ls2str(l->l.state));
			if (l->l.owner_nodeid) {
				fprintf(fp, ", owner %d:%d:%d", l->l.owner_nodeid,l->l.owner_pid, l->l.owner_tid);
				if (l->l.owner_nodeid == my_node_id &&
				    l->l.state == LOCK_HELD)
					fprintf(fp, ", Local ID %d", l->l.local_id);
			}
			fprintf(fp, "\n");
		}
		fprintf(fp, "\n");
	}
	if (requests) {
		fprintf(fp, "Requests\n");
		fprintf(fp, "========\n");
		list_for(&requests, r, x) {
			fprintf(fp, "  %s: %s", r->l.resource, rq2str(r->l.state));
			if (r->l.owner_nodeid) {
				fprintf(fp, ", from %d:%d:%d", r->l.owner_nodeid,r->l.owner_pid, r->l.owner_tid);
			}
			fprintf(fp, "\n");
		}
		fprintf(fp, "\n");
	}
	if (messages) {
		fprintf(fp, "Message History\n");
		fprintf(fp, "======= =======\n");
		list_for(&messages, s, x) {
			switch(s->m.request) {
			case MSG_CONFCHG:
				fprintf(fp, "  CONFIG CHANGE\n");
				break;
			case MSG_PURGE:
				fprintf(fp, "  PURGE for %d:%d\n", s->m.owner_nodeid, s->m.owner_pid);
				break;
			case MSG_JOIN:
				fprintf(fp, "  JOIN %d\n", s->m.owner_nodeid);
				break;
			default:
				fprintf(fp, "  %s: %s %d:%d:%d\n", rq2str(s->m.request), s->m.resource, s->m.owner_nodeid, s->m.owner_pid, s->m.owner_tid);
				break;
			}
		}
		fprintf(fp, "\n");
	}
}


static void
old_msg(struct cpg_lock_msg *m)
{
	struct msg_node *n;

	n = do_alloc(sizeof(*n));
	memcpy(&n->m, m, sizeof(n->m));
	list_append(&messages, n);
	if (message_count < 20) {
		++message_count;
	} else {
		n = messages;
		list_remove(&messages, n);
		free(n);
	}
}


static void
insert_client(int fd)
{
	struct client_node *n = NULL;

	n = do_alloc(sizeof(*n));
	n->fd = fd;
	list_append(&clients, n);
}


/* forward request from client */
static int
send_lock_msg(struct cpg_lock_msg *m)
{
	struct iovec iov;
	int ret;

	iov.iov_base = m;
	iov.iov_len = sizeof (*m);

	ret = cpg_mcast_joined(cpg, CPG_TYPE_AGREED, &iov, 1);
	if (ret != CPG_OK)
		return -1;
	return 0;
}


/* forward request from client */
static int
send_lock(struct cpg_lock_msg *m)
{
	m->owner_nodeid = my_node_id;

	return send_lock_msg(m);
}


static int  
send_grant(struct request_node *n)
{
	struct cpg_lock_msg m;

	cpgl_debug("-> sending grant %s to %d:%d:%d\n",
		n->l.resource, n->l.owner_nodeid, n->l.owner_pid, n->l.owner_tid);

	memset(&m, 0, sizeof(m));
	strncpy(m.resource, n->l.resource, sizeof(m.resource));
	m.request = MSG_GRANT;
	m.owner_nodeid = n->l.owner_nodeid;
	m.owner_pid = n->l.owner_pid;
	m.owner_tid = n->l.owner_tid;
	
	return send_lock_msg(&m);
}


static int  
send_nak(struct cpg_lock_msg *m)
{
	m->request = MSG_NAK;

	return send_lock_msg(m);
}


static int  
send_join(void)
{
	struct cpg_lock_msg m;

	m.request = MSG_JOIN;
	m.owner_nodeid = my_node_id;
	return send_lock_msg(&m);
}



static int
send_unlock(struct cpg_lock_msg *m)
{
	m->request = MSG_UNLOCK;
	return send_lock_msg(m);
}


/*
 * Grant the lock in this request node to the next
 * waiting client.
 */
static int
grant_next(struct cpg_lock_msg *m)
{
	struct request_node *r;
	int x;

	list_for(&requests, r, x) {
		if (strcmp(m->resource, r->l.resource))
			continue;

		/* Send grant */
		if (r->l.state == LOCK_PENDING) {
			cpgl_debug("LOCK %s: grant to %d:%d:%d\n", m->resource,
			       r->l.owner_nodeid, r->l.owner_pid, r->l.owner_tid);
			/* don't send dup grants */
			r->l.state = LOCK_HELD;
			send_grant(r);
		}
		return 1;
	}

	return 0;
}


static void
purge_requests(uint32_t nodeid, uint32_t pid)
{
	struct request_node *r;
	int found = 0, count = 0, x = 0;

	do {
		found = 0;
		list_for(&requests, r, x) {
			if (r->l.owner_nodeid != nodeid ||
			    (pid && 
			     r->l.owner_pid != pid))
				continue;
		
			list_remove(&requests, r);
			free(r);
			found = 1;
			++count;
			break;
		}
	} while (found);

	if (count) {
		if (pid) {
			cpgl_debug("RECOVERY: purged %d requests from %d:%d\n", count, nodeid, pid);
		} else {
			cpgl_debug("RECOVERY: purged %d requests from node %d\n", count, nodeid);
		}
	}
}



static void
del_client(int fd)
{
	struct cpg_lock_msg m;
	struct client_node *n;
	struct lock_node *l;
	int x, pid = 0, recovered = 0;

	list_for(&clients, n, x) {
		if (n->fd == fd) {
			list_remove(&clients, n);
			close(n->fd);
			pid = n->pid;
			free(n);
			break;
		}
	}

	if (!pid)
		return;

	cpgl_debug("RECOVERY: Looking for locks held by PID %d\n", pid);

	/* This may not be needed */
	purge_requests(my_node_id, pid);

	memset(&m, 0, sizeof(m));
	m.request = MSG_PURGE;
	m.owner_nodeid = my_node_id;
	m.owner_pid = pid;

	send_lock_msg(&m);

	list_for(&locks, l, x) {
		if (l->l.owner_nodeid != my_node_id ||
		    l->l.owner_pid != pid ||
		    l->l.state != LOCK_HELD)
			continue;

		cpgl_debug("RECOVERY: Releasing %s \n", l->l.resource);

		l->l.state = LOCK_FREE;
		strncpy(m.resource, l->l.resource, sizeof(m.resource));
		if (grant_next(&m) == 0)
			send_unlock(&m);
	}

	if (recovered) {
		cpgl_debug("RECOVERY: %d locks from local PID %d\n", recovered, pid);
	}
	cpgl_debug("RECOVERY: Complete\n");
}


static void
del_node(uint32_t nodeid)
{
	struct cpg_lock_msg m;
	struct lock_node *l;
	int x, recovered = 0, granted = 0;

	if (group_members->nodeid != my_node_id)
		return;

	cpgl_debug("RECOVERY: I am oldest node in the group, recovering locks\n");

	/* pass 1: purge outstanding requests from this node. */

	/* This may not be needed */
	purge_requests(nodeid, 0);

	memset(&m, 0, sizeof(m));
	m.request = MSG_PURGE;
	m.owner_nodeid = nodeid;
	m.owner_pid = 0;

	send_lock_msg(&m);

	list_for(&locks, l, x) {
		if (l->l.owner_nodeid == nodeid && 
		    l->l.state == LOCK_HELD) {
			cpgl_debug("RECOVERY: Releasing %s held by dead node %d\n",
				l->l.resource, nodeid);

			l->l.state = LOCK_FREE;
			strncpy(m.resource, l->l.resource, sizeof(m.resource));
			if (grant_next(&m) == 0)
				send_unlock(&m);
			++recovered;
		} else if (l->l.state == LOCK_FREE) {
			if (grant_next(&m) == 0)
				send_unlock(&m);
			++granted;
		}
	}

	if (recovered) {
		cpgl_debug("RECOVERY: %d locks from node %d\n", recovered, nodeid);
	}
	if (granted) {
		cpgl_debug("RECOVERY: %d pending locks granted\n", granted);
	}

	cpgl_debug("RECOVERY: Complete\n");
}


static int
client_fdset(fd_set *set)
{
	int max = -1, x = 0;
	struct client_node *n;

	FD_ZERO(set);

	list_for(&clients, n, x) {
		FD_SET(n->fd, set);
		if (n->fd > max)
			max = n->fd;
	}

	if (!x)
		return 0;

	return max;
}


static struct client_node *
find_client(int pid)
{
	int x;
	struct client_node *n;

	list_for(&clients, n, x) {
		if (n->pid == pid)
			return n;
	}

	return NULL;
}


#if 0
static void
send_fault(const char *resource)
{
	struct cpg_lock_msg m;

	strncpy(m.resource, resource, sizeof(m.resource));
	m.request = MSG_HALT;
	m.owner_pid = 0;
	m.owner_nodeid = my_node_id;

	send_lock_msg(&m);
}
#endif


static int
grant_client(struct lock_node *l)
{
	struct client_node *c;
	struct cpg_lock_msg m;

	memset(&m, 0, sizeof(m));
	strncpy(m.resource, l->l.resource, sizeof(m.resource));
	m.request = MSG_GRANT;
	m.owner_pid = l->l.owner_pid;
	m.owner_tid = l->l.owner_tid;
	l->l.local_id = ++local_lockid;
	m.lockid = l->l.local_id;
	m.owner_nodeid = my_node_id;

	c = find_client(l->l.owner_pid);
	if (!c) {
		cpgl_debug("can't find client for pid %d\n", l->l.owner_pid);
		return 1;
	}

	if (c->fd < 0) {
		cpgl_debug(" Client has bad fd\n");
		return -1;
	}

	if (write_retry(c->fd, &m, sizeof(m), NULL) < 0) {
		/* no client anymore; drop and send to next guy XXX */
		/* This should be handled by our main loop */
		//cpgl_debug("Failed to notify client!\n");
	}

	return 0;
}


static int
nak_client(struct request_node *l)
{
	struct client_node *c;
	struct cpg_lock_msg m;

	memset(&m, 0, sizeof(m));
	strncpy(m.resource, l->l.resource, sizeof(m.resource));
	m.request = MSG_NAK;
	m.owner_pid = l->l.owner_pid;
	m.owner_tid = l->l.owner_tid;
	m.owner_nodeid = my_node_id;

	c = find_client(l->l.owner_pid);
	if (!c) {
		cpgl_debug("can't find client for pid %d\n", l->l.owner_pid);
		return 1;
	}

	if (c->fd < 0) {
		cpgl_debug(" Client has bad fd\n");
		return -1;
	}

	if (write_retry(c->fd, &m, sizeof(m), NULL) < 0) {
		/* no client anymore; drop and send to next guy XXX */
		/* This should be handled by our main loop */
		//cpgl_debug("Failed to notify client!\n");
	}

	return 0;
}

static int
queue_request(struct cpg_lock_msg *m)
{
	struct request_node *r;

	r = do_alloc(sizeof(*r));
	strncpy(r->l.resource, m->resource, sizeof(r->l.resource));
	r->l.owner_nodeid = m->owner_nodeid;
	r->l.owner_pid = m->owner_pid;
	r->l.owner_tid = m->owner_tid;
	r->l.state = LOCK_PENDING;

	list_insert(&requests, r);
	return 0;
}


static int
process_lock(struct cpg_lock_msg *m)
{
	struct lock_node *l;
	int x;

	if (!joined)
		return 0;

	cpgl_debug("LOCK %s: queue for %d:%d:%d\n", m->resource,
	       m->owner_nodeid, m->owner_pid, m->owner_tid);
	queue_request(m);

	list_for(&locks, l, x) {
		if (strcmp(m->resource, l->l.resource))
			continue;

		/* if it's owned locally, we need send a
                   GRANT to the first node on the request queue */
		if (l->l.owner_nodeid == my_node_id) {
			if (l->l.state == LOCK_FREE) {
				/* Set local state to PENDING to avoid double-grants */
				l->l.state = LOCK_PENDING;
				grant_next(m);
			} else {
				/* state is PENDING or HELD */
				if (m->flags & FL_TRY) {
					/* nack to client if needed */
					send_nak(m);
				}
			}
		}

		
		return 0;
	}

	l = do_alloc(sizeof(*l));
	strncpy(l->l.resource, m->resource, sizeof(l->l.resource));
	l->l.state = LOCK_FREE;
	list_insert(&locks, l);

	if (group_members->nodeid == my_node_id) {
		/* Allocate a lock structure and immediately grant */
		l->l.state = LOCK_PENDING;
		if (grant_next(m) == 0)
			l->l.state = LOCK_FREE;
	}

	return 0;
}


static int
is_member(uint32_t nodeid)
{
	struct member_node *n;
	int x;

	list_for(&group_members, n, x) {
		if (n->nodeid == nodeid)
			return 1;
	}

	return 0;
}


static int
process_grant(struct cpg_lock_msg *m, uint32_t nodeid)
{
	struct lock_node *l;
	struct request_node *r;
	int x, y;

	if (!joined)
		return 0;

	list_for(&locks, l, x) {
		if (strcmp(m->resource, l->l.resource))
			continue;

		if (l->l.state == LOCK_HELD) {
			if (m->owner_pid == 0 ||
			    m->owner_nodeid == 0) {
				cpgl_debug("GRANT averted\n");
				return 0;
			}
		} else {
			l->l.state = LOCK_HELD;
		}

		cpgl_debug("GRANT %s: to %d:%d:%d\n",
			m->resource, m->owner_nodeid,
			m->owner_pid, m->owner_tid);

		l->l.owner_nodeid = m->owner_nodeid;
		l->l.owner_pid = m->owner_pid;
		l->l.owner_tid = m->owner_tid;

		list_for(&requests, r, y) {
			if (strcmp(r->l.resource, m->resource))
				continue;

			if (r->l.owner_nodeid == m->owner_nodeid &&
			    r->l.owner_pid == m->owner_pid &&
			    r->l.owner_tid == m->owner_tid) {
				list_remove(&requests, r);
				free(r);
				break;
			}
		}

		/* granted lock */
		if (l->l.owner_nodeid == my_node_id) {
			if (grant_client(l) != 0) {
				/* Grant to a nonexistent PID can
				   happen because we may have a pending
				   request after a fd was closed.
				   since we process on delivery, we
				   now simply make an unlock request
				   and move on */
				purge_requests(my_node_id, l->l.owner_pid);
				if (grant_next(m) == 0)
					send_unlock(m);
				return 0;
			}
		}

		/* What if node has died with a GRANT in flight? */
		if (group_members->nodeid == my_node_id &&
		    !is_member(l->l.owner_nodeid)) {

			cpgl_debug("GRANT to non-member %d; giving to next requestor\n",
				l->l.owner_nodeid);
			
			l->l.state = LOCK_FREE;
			if (grant_next(m) == 0)
				send_unlock(m);
			return 0;
		}
		return 0;
	}

	/* Record lock state since we now know it */
	/* Allocate a lock structure */
	l = do_alloc(sizeof(*l));
	strncpy(l->l.resource, m->resource, sizeof(l->l.resource));
	l->l.state = LOCK_HELD;
	l->l.owner_nodeid = m->owner_nodeid;
	l->l.owner_pid = m->owner_pid;
	l->l.owner_tid = m->owner_tid;
	list_insert(&locks, l);

	return 0;
}


static int
process_nak(struct cpg_lock_msg *m, uint32_t nodeid)
{
	struct request_node *r = NULL;
	int y;

	if (!joined)
		return 0;

	list_for(&requests, r, y) {
		if (strcmp(r->l.resource, m->resource))
			continue;

		if (r->l.owner_nodeid == m->owner_nodeid &&
		    r->l.owner_pid == m->owner_pid &&
		    r->l.owner_tid == m->owner_tid) {
			list_remove(&requests, r);
			if (r->l.owner_nodeid == my_node_id) {
				if (nak_client(r) != 0) {
					purge_requests(my_node_id, r->l.owner_pid);
				}
			}
			free(r);
			break;
		}
	}

	return 0;
}


static int
process_unlock(struct cpg_lock_msg *m, uint32_t nodeid)
{
	struct lock_node *l;
	int x;

	if (!joined)
		return 0;

	list_for(&locks, l, x) {
		if (l->l.state != LOCK_HELD)
			continue;
		if (strcmp(m->resource, l->l.resource))
			continue;

		/* Held lock... if it's local, we need send a
                   GRANT to the first node on the request queue */
		if (l->l.owner_nodeid == m->owner_nodeid &&
		    l->l.owner_pid == m->owner_pid) {
			cpgl_debug("UNLOCK %s: %d:%d:%d\n",
				m->resource, m->owner_nodeid, m->owner_pid, m->owner_tid);
			l->l.state = LOCK_FREE;
			if (l->l.owner_nodeid == my_node_id) {
				if (grant_next(m) != 0)
					l->l.state = LOCK_PENDING;
			}
		}
	}

	return 0;
}


static int
find_lock(struct cpg_lock_msg *m)
{
	struct lock_node *l;
	int x;

	if (m->resource[0] != 0)
		return 0;

	list_for(&locks, l, x) {
		if (m->lockid == l->l.local_id) {
			strncpy(m->resource, l->l.resource, sizeof(m->resource));
			cpgl_debug("LOCK %d -> %s\n", m->lockid, m->resource);
			m->owner_nodeid = l->l.owner_nodeid;
			m->owner_pid = l->l.owner_pid;
			m->owner_tid = l->l.owner_tid;
			m->lockid = 0;
			return 0;
		}
	}

	return 1;
}


static int
process_join(struct cpg_lock_msg *m, uint32_t nodeid, uint32_t pid)
{
	struct member_node *n;
	int x;

	list_for(&group_members, n, x) {
		if (n->nodeid == nodeid) {
			cpgl_debug("IGNORING JOIN from existing member %d.%d (%d.%d)\n",
				nodeid, pid, nodeid, n->pid);
			return 0;
		}
	}

	n = do_alloc(sizeof(*n));
	n->nodeid = nodeid;
	n->pid = pid;
	cpgl_debug("JOIN: node %d.%u", n->nodeid, n->pid);
	if (nodeid == my_node_id) {
		cpgl_debug(" (self)");
		joined = 1;
	}
	total_members++;
	cpgl_debug("\n");
	list_insert(&group_members, n);

	return 0;
}


static int
process_request(struct cpg_lock_msg *m, uint32_t nodeid, uint32_t pid)
{
	if (m->request == MSG_HALT) {
		cpgl_debug("FAULT: Halting operations; see node %d\n", m->owner_nodeid);
		while (1) 
			sleep(30);
	}

	old_msg(m);

	switch (m->request) {
	case MSG_LOCK:
		process_lock(m);
		break;
	case MSG_NAK:
		process_nak(m, nodeid);
		break;
	case MSG_GRANT:
		process_grant(m, nodeid);
		break;
	case MSG_UNLOCK:
		process_unlock(m, nodeid);
		break;
	case MSG_PURGE:
		purge_requests(m->owner_nodeid, m->owner_pid);
		break;
	case MSG_JOIN:
		process_join(m, nodeid, pid);
		break;
	}

	return 0;
}


static void
cpg_deliver_func(cpg_handle_t h,
		 const struct cpg_name *group_name,
		 uint32_t nodeid,
		 uint32_t pid,
		 void *msg,
		 size_t msglen)
{

	if (msglen != sizeof(struct cpg_lock_msg)) {
		cpgl_debug("Invalid message size %d\n", (int)msglen);
	}

	process_request((struct cpg_lock_msg *)msg, nodeid, pid);
}


static void
cpg_config_change(cpg_handle_t h,
		  const struct cpg_name *group_name, 
		  const struct cpg_address *members, size_t memberlen,
		  const struct cpg_address *left, size_t leftlen,
		  const struct cpg_address *join, size_t joinlen)
{
	struct member_node *n;
	size_t x, y;
	struct cpg_lock_msg m;
	int cpglock_members_removed = 0;

	memset(&m, 0, sizeof(m));
	strncpy(m.resource, "(none)", sizeof(m.resource));
	m.request = MSG_CONFCHG;

	old_msg(&m);

	if (total_members == 0) {
		cpgl_debug("JOIN: Setting up initial node list\n");
		for (x = 0; x < memberlen; x++) {
			for (y = 0; y < joinlen; y++) {
				if (join[y].nodeid == members[x].nodeid)
					continue;
				if (members[x].nodeid == my_node_id)
					continue;

				n = do_alloc(sizeof(*n));
				n->nodeid = members[x].nodeid;
				n->pid = members[x].pid;
				cpgl_debug("JOIN: node %d.%u\n", n->nodeid, n->pid);
				list_insert(&group_members, n);
			}
		}
		cpgl_debug("JOIN: Done\n");

		total_members = memberlen;
	}

	//cpgl_debug("members %d now, %d joined, %d left\n", memberlen, joinlen, leftlen);
#if 0

	/* XXX process join on receipt of JOIN message rather than here 
	   since ordered delivery is agreed, this prevents >1 member from
	   believing it is the oldest host */
	for (x = 0; x < joinlen; x++) {
		n = do_alloc(sizeof(*n));
		n->nodeid = join[x].nodeid;
		cpgl_debug("ADD: node %d\n", n->nodeid);
		list_insert(&group_members, n);
	}
#endif

	for (x = 0; x < leftlen; x++) {
		list_for(&group_members, n, y) {
			if (n->nodeid == left[x].nodeid) {
				if (n->pid == left[x].pid) {
					if (n->nodeid == my_node_id) {
						cpgl_debug("Received DELETE message for self. Exiting.");
						exit(0);
					}
					list_remove(&group_members, n);
					cpgl_debug("DELETE: node %d.%u\n", n->nodeid, n->pid);
					del_node(n->nodeid);
					cpglock_members_removed++;
					free(n);
					break;
				} else {
					cpgl_debug("DUPE NODE %d LEFT (%u != %u)\n",
						n->nodeid, n->pid, left[x].pid);
				}
			}
		}
	}

	total_members -= cpglock_members_removed;
	if (total_members < 0)
		total_members = 0;

#if 0
	cpgl_debug("MEMBERS:");
	list_for(&group_members, n, y) {
		cpgl_debug(" %d.%u", n->nodeid, n->pid);
	}
	cpgl_debug("\n");
#endif

	return;
}


static cpg_callbacks_t my_callbacks = {
	.cpg_deliver_fn = cpg_deliver_func,
	.cpg_confchg_fn = cpg_config_change
};

static int
cpg_fin(void)
{
	struct cpg_name gname;
	
	errno = EINVAL;

	gname.length = snprintf(gname.value,
				sizeof(gname.value),
				CPG_LOCKD_NAME);
	if (gname.length >= sizeof(gname.value)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	if (gname.length <= 0)
		return -1;

	cpg_leave(cpg, &gname);
	cpg_finalize(cpg);

	return 0;
}

static int
cpg_init(void)
{
	struct cpg_name gname;
	struct cpg_address member_list[64];
	int cpg_member_list_len = 0;
	int i, ret;
	
	errno = EINVAL;

	gname.length = snprintf(gname.value,
				sizeof(gname.value),
				CPG_LOCKD_NAME);
	if (gname.length >= sizeof(gname.value)) {
		errno = ENAMETOOLONG;
		return -1;
	}

	if (gname.length <= 0)
		return -1;

	memset(&cpg, 0, sizeof(cpg));
	if (cpg_initialize(&cpg, &my_callbacks) != CPG_OK) {
		perror("cpg_initialize");
		return -1;
	}

	if (cpg_join(cpg, &gname) != CPG_OK) {
		perror("cpg_join");
		return -1;
	}

	cpg_local_get(cpg, &my_node_id);

	ret = cpg_membership_get(cpg, &gname, member_list, &cpg_member_list_len);
	if (ret != CPG_OK) {
		fprintf(stderr, "cpg_membership_get() failed: %s", strerror(errno));
		cpg_fin();
		return -1;
	}

	for (i = 0 ; i < cpg_member_list_len ; i++) {
		if (member_list[i].nodeid == my_node_id) {
			fprintf(stderr, "nodeid %d already in group with PID %u\n",
				member_list[i].nodeid, member_list[i].pid);
			cpg_fin();
			return -1;
		}
	}

	return 0;
}

int
main(int argc, char **argv)
{
	fd_set rfds;
	int fd;
	int cpgfd;
	int afd = -1;
	int cman_fd;
	int n,x;
	int nofork = 0;
	int wait_for_quorum = 1;
	int wait_for_fencing = 1;
	int opt;
	int ret;
	struct cpg_lock_msg m;
	struct client_node *client;
	cman_handle_t cman_handle = NULL;
	cman_node_t my_node;

	while ((opt = getopt(argc, argv, "FQfh")) != EOF) {
		switch (opt) {
			case 'F':
				wait_for_fencing = 0;
				break;
			case 'Q':
				wait_for_quorum = 0;
				break;
			case 'f':
				nofork = 1;
				break;
			case 'h':
				printf("Usage: %s [options]\n\
 -F      Don't wait for the current node to join the fencing domain at startup\n\
 -Q      Don't wait for quorum formation at startup\n\
 -f      Don't daemonize\n\
 -h      Print this help message\n",
					argv[0]);
				return 0;
			default:
				return -1;
		}
	}

	signal(SIGPIPE, SIG_IGN);

	cman_connect(&cman_handle);
	if (cman_handle == NULL) {
		fprintf(stderr, "Unable to connect to cman\n");
		return -1;
	}

	if (wait_for_quorum)
		wait_for_quorum_formation(cman_handle);

	memset(&my_node, 0, sizeof(my_node));
	cman_get_node(cman_handle, CMAN_NODEID_US, &my_node);

	if (my_node.cn_nodeid == 0) {
		fprintf(stderr, "Unable to get our cluster node ID\n");
		cman_finish(cman_handle);
		return -1;
	}

	if (wait_for_fencing)
		wait_for_fencing_join(my_node.cn_nodeid);

	memset(&cman_nodes, 0, sizeof(cman_nodes));
	cman_node_count = 0;

	ret = cman_get_nodes(cman_handle,
			sizeof(cman_nodes) / sizeof(cman_nodes[0]),
			&cman_node_count, cman_nodes);
	if (ret < 0) {
		fprintf(stderr, "Unable to get cman nodes list\n");
		cman_finish(cman_handle);
		return -1;
	}

	cman_fd = cman_get_fd(cman_handle);
	if (cman_fd < 0) {
		fprintf(stderr, "Error: cman fd is %d\n", cman_fd);
		cman_finish(cman_handle);
		return -1;
	}

	cman_start_notification(cman_handle, cman_callback);

	if (cpg_init() < 0) {
		fprintf(stderr, "Unable to join CPG group\n");
		cman_stop_notification(cman_handle);
		cman_finish(cman_handle);
		return -1;
	}

	assert(my_node.cn_nodeid == my_node_id);

	fd = sock_listen(CPG_LOCKD_SOCK);
	if (fd < 0) {
		fprintf(stderr, "Error connecting to %s: %s\n",
			CPG_LOCKD_SOCK, strerror(errno));
		cman_stop_notification(cman_handle);
		cman_finish(cman_handle);
		cpg_fin();
		return -1;
	}

	if (!nofork)
		daemon_init((char *) "cpglockd");

	cpg_local_get(cpg, &my_node_id);
	cpg_fd_get(cpg, &cpgfd);
	if (send_join() < 0)
		return -1;

	while (1) {
		struct timeval tv;
		struct pending_fence_node *pf_node;

		FD_ZERO(&rfds);
		x = client_fdset(&rfds);
		FD_SET(fd, &rfds);
		if (fd > x)
			x = fd;
		FD_SET(cpgfd, &rfds);
		if (cpgfd > x)
			x = cpgfd;
		cman_fd = cman_get_fd(cman_handle);
		FD_SET(cman_fd, &rfds);
		if (cman_fd > x)
			x = cman_fd;

		tv.tv_sec = 0;
		tv.tv_usec = 500000;

		n = select_retry(x+1, &rfds, NULL, NULL, &tv);
		if (n < 0) {
			fprintf(stderr, "Error: select: %s\n", strerror(errno));
			return -1;
		}

		if (FD_ISSET(cman_fd, &rfds)) {
			if (cman_dispatch(cman_handle, CMAN_DISPATCH_ALL) < 0) {
				fprintf(stderr, "Fatal: cman_dispatch() failed: %s\n",
					strerror(errno));
				return -1;
			}
			--n;
		}

		if (shutdown_requested) {
			cman_replyto_shutdown(cman_handle, 1);
			fprintf(stderr, "Fatal: cman requested shutdown.\n");
			return -1;
		}

		fence_check:
		list_for(&pending_fencing, pf_node, x) {
			int victim;
			uint64_t lft;
			struct fenced_node fn;

			ret = fenced_node_info(pf_node->nodeid, &fn);
			if (ret < 0) {
				cpgl_debug("Unable to get last fenced time for node %d\n",
					pf_node->nodeid);
				victim = 1;
				lft = 0;
			} else {
				victim = fn.victim;
				lft = fn.last_fenced_time;
			}

			if (lft > pf_node->fail_time) {
				cpgl_debug("Fencing for node %d finished at %ld (>%ld)\n",
					pf_node->nodeid, lft, pf_node->fail_time);
				list_remove(&pending_fencing, pf_node);
				free(pf_node);
				goto fence_check;
			}

			/*
			** If node A either fails or leaves the cluster cleanly while
			** fencing is outstanding on another node (or nodes), we aren't
			** able to determine whether A has failed or left cleanly until
			** fencing has completed for the nodes that have left or failed
			** prior to A while any fencing is outstanding. The first entry
			** in this list will be the node that has left or failed longer
			** ago than any others in the list. If fenced has not set
			** victim to 1 by now, we can deduce it has left cleanly, and we
			** don't need to wait for it.
			*/
			if (!victim && !x) {
				cpgl_debug("First entry in list and victim == 0, removing from pending fencing\n");
				list_remove(&pending_fencing, pf_node);
				free(pf_node);
				goto fence_check;
			}
		}

		/* While fencing is pending for any nodes pause lock activity. */
		if (pending_fencing != NULL) {
			usleep(500000);
			continue;
		}

		if (FD_ISSET(fd, &rfds)) {
			afd = accept(fd, NULL, NULL);
			insert_client(afd);
			--n;
		}

		if (FD_ISSET(cpgfd, &rfds)) {
			if (cpg_dispatch(cpg, CPG_DISPATCH_ALL) != CPG_OK) {
				fprintf(stderr, "Fatal: Lost CPG connection.\n");
				return -1;
			}
			--n;
		}

		if (n <= 0)
			continue;

		do {
			list_for(&clients, client, x) {
				if (!FD_ISSET(client->fd, &rfds))
					continue;
				--n;
				if (read_retry(client->fd, &m, sizeof(m), NULL) < 0) {
					cpgl_debug("Closing client fd %d pid %d: %d\n",
					       client->fd, client->pid, errno);
			
					del_client(client->fd);
					break;
				}

				/* send lock request */
				/* XXX check for dup connection */
				if (m.request == MSG_LOCK) {
					client->pid = m.owner_pid;
					send_lock(&m);
				}

				if (m.request == MSG_UNLOCK) {
					//cpgl_debug("Unlock from fd %d\n", client->fd);
					find_lock(&m);
					if (grant_next(&m) == 0)
						send_unlock(&m);
				}
	
				if (m.request == MSG_DUMP) {
					FILE *fp = fdopen(client->fd, "w");

					list_remove(&clients, client);
					dump_state(fp);
					fclose(fp);
					close(client->fd);
					free(client);
					break;
				}
			}
		} while (n);
	}

	cman_stop_notification(cman_handle);
	cman_finish(cman_handle);
	cpg_fin();

	return 0;
}
