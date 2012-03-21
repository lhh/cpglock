#include <stdio.h>
#include <cpglock.h>
#include <signal.h>
#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <stdlib.h>

int
main(int argc, char **argv)
{
	cpg_lock_handle_t h;
	int n = 0, count = 0;
	char buf[32];
	struct cpg_lock *l;

	if (argc < 2) {
		printf("usage: %s count\n", argv[0]);
		return 1;
	}

	count = atoi(argv[1]);
	if (count <= 0) {
		printf("count must be > 0\n");
		return 1;
	}

	if (cpg_lock_init(&h) < 0) {
		perror("cpg_lock_init");
		return 1;
	}

	l = malloc(sizeof(*l) * count);

	for (n = 0; n < count; n++) {
		snprintf(buf, sizeof(buf), "%d", n);

		if (cpg_lock(h, buf, 0, &l[n]) < 0) {
			perror("cpg_lock");
			return 1;
		}
	}

	printf("%d locks taken, press <enter> to release\n", count);
	getc(stdin);

	for (n = 0; n < count; n++) {
		snprintf(buf, sizeof(buf), "%d", n);

		if (cpg_unlock(h, &l[n]) < 0) {
			perror("cpg_unlock");
			return 1;
		}
	}

	cpg_lock_fin(h);

	return 0;
}
