#include <stdio.h>
#include <cpglock.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

static cpg_lock_handle_t h;


struct arg_s {
	char *outer;
	char *inner;
};


static void *
bg(void *arg)
{
	struct cpg_lock i, o;
	char *outer = ((struct arg_s *)arg)->outer;
	char *inner = ((struct arg_s *)arg)->inner;

	while (1) {
		printf("BG lock requested\n");
		if (cpg_lock(h, inner, 0, &i) < 0) {
			perror("cpg_lock[bg]");
			return NULL;
		}

		if (cpg_lock(h, outer, 0, &o) < 0) {
			perror("cpg_lock[bg]");
			return NULL;
		}

		usleep(random()&32767);
	
		if (cpg_unlock(h, &o) < 0) {
			perror("cpg_unlock");
			return NULL;
		}

		if (cpg_unlock(h, &i) < 0) {
			perror("cpg_lock[bg]");
			return NULL;
		}
	}

	return NULL;
}
	


int
main(int argc, char **argv)
{
	struct cpg_lock l;
	struct arg_s *args;
	int x, y;

	pthread_t *th;

	if (argc < 2) {
		printf("usage: %s count\n", argv[0]);
		return 1;
	}

	y = atoi(argv[1]);
	if (y <= 0) {
		printf("Invalid count %s\n", argv[1]);
		return 1;
	}

	th = malloc(y * sizeof(pthread_t));

	if (cpg_lock_init(&h) < 0) {
		perror("cpg_lock_init");
		return 1;
	}

	for (x = 0; x < y; x++) {

		args = malloc(sizeof(*args));

		args->outer = strdup("outer");
		args->inner = malloc(32);
		snprintf(args->inner, 32, "inner%d", x);

		pthread_create(&th[x], NULL, bg, args);
		
	}


	while (1) {

		printf("FG lock released\n");
		if (cpg_lock(h, "outer", 0, &l) < 0) {
			perror("cpg_lock");
			return -1;
		}

		printf("FG lock granted\n");
		if (cpg_unlock(h, &l) < 0) {
			perror("cpg_unlock");
			return 1;
		}
		printf("FG lock released\n");
	}

	for (x = 0; x < y; x++) {
		pthread_join(th[x], NULL);
	}

	cpg_lock_fin(h);

	return 0;
}
