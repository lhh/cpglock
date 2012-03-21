#include <stdio.h>
#include <cpglock.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>

static pthread_t th;
static cpg_lock_handle_t h;


static void *
bg(void *arg)
{
	struct cpg_lock l;
	char *ln = (char *)arg;

	printf("BG lock requested\n");
	if (cpg_lock(h, ln, 0, &l) < 0) {
		perror("cpg_lock[bg]");
		return NULL;
	}

	printf("BG lock granted\n");
	
	if (cpg_unlock(h, &l) < 0) {
		perror("cpg_unlock");
		return NULL;
	}

	printf("BG lock released\n");

	return NULL;
}
	


int
main(int argc, char **argv)
{
	struct cpg_lock l;

	if (argc < 2) {
		printf("usage: %s lockname\n", argv[0]);
		return 1;
	}

	if (cpg_lock_init(&h) < 0) {
		perror("cpg_lock_init");
		return 1;
	}

	printf("Acquiring lock on %s...", argv[1]);
        fflush(stdout);

	if (cpg_lock(h, argv[1], 0, &l) < 0) {
		perror("cpg_lock");
		return 1;
	}

        printf("OK\npress <enter> to unlock\n");

	pthread_create(&th, NULL, bg, argv[1]);

        getc(stdin);

	if (cpg_unlock(h, &l) < 0) {
		perror("cpg_unlock");
		return 1;
	}

	pthread_join(th, NULL);

	cpg_lock_fin(h);

	return 0;
}
