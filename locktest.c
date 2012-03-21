#include <stdio.h>
#include <cpglock.h>
#include <errno.h>
#include <string.h>

int
main(int argc, char **argv)
{
	cpg_lock_handle_t h;
	struct cpg_lock l;

	if (argc < 2) {
		cpg_lock_dump(stdout);
		return 0;
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

        printf("OK, local id %d\npress <enter> to unlock\n", l.local_id);
        getc(stdin);

	memset(l.resource, 0, sizeof(l.resource));

	if (cpg_unlock(h, &l) < 0) {
		perror("cpg_unlock");
		return 1;
	}

	cpg_lock_fin(h);

	return 0;
}
