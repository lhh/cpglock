#include <stdio.h>
#include <cpglock.h>
#include <signal.h>
#include <errno.h>
#include <string.h>

static int running = 1;
static void
inter(int sig)
{
	running = 0;
}


int
main(int argc, char **argv)
{
	cpg_lock_handle_t h;
	struct cpg_lock l;
	int n = 0;

	if (argc < 2) {
		printf("usage: %s lockname\n", argv[0]);
		return 1;
	}

	signal(SIGINT, inter);

	if (cpg_lock_init(&h) < 0) {
		perror("cpg_lock_init");
		return 1;
	}
	
	while(running) {
		printf("lock ... ");
		fflush(stdout);
		if (cpg_lock(h, argv[1], 0, &l) < 0) {
			perror("cpg_lock");
			return 1;
		}

		printf("unlock");
		if (cpg_unlock(h, &l) < 0) {
			perror("cpg_unlock");
			return 1;
		}
		++n;
		printf("\n");
	}

	cpg_lock_fin(h);

	printf("%s taken/released %d times\n", argv[0], n);

	return 0;
}
