#ifndef _SOCK_H
#define _SOCK_H

ssize_t read_retry(int sockfd, void *buf, size_t count,
		   struct timeval * timeout);
int select_retry(int fdmax, fd_set * rfds, fd_set * wfds, fd_set * xfds,
		 struct timeval *timeout);
ssize_t write_retry(int fd, void *buf, size_t count,
		   struct timeval *timeout);

int sock_listen(const char *sockpath);
int sock_connect(const char *sockpath, int tout);
int sock_accept(int fd);
void hexdump(const void *buf, size_t len);

void *do_alloc(size_t);

#endif
