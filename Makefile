CFLAGS=-Wall -Wformat=2 -Wshadow -Wmissing-prototypes -Wstrict-prototypes -Wdeclaration-after-statement -Wpointer-arith -Wwrite-strings -Wcast-align -Wbad-function-cast -Wmissing-format-attribute -Wformat-security -Wformat-nonliteral -Wno-long-long -Wno-strict-aliasing -Wmissing-declarations -ggdb3 -MMD -Werror

all: libcpglock.so cpglockd locktest lock lockstress many thread thread2 try many2

cpglockd: sock.o cpglockd.o
	gcc -o $@ $^ -lcpg

libcpglock.so: sock.o libcpglock.o 
	gcc -o $@ $^ -shared 

locktest: libcpglock.so locktest.o
	gcc -o locktest -L. -lcpglock locktest.o -lpthread

lock: locktest.o sock.o libcpglock.o
	gcc -o $@ -L. $^ -lpthread

lockstress: lockstress.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

many: many.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

thread: thread.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

thread2: thread2.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

many2: many2.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

try: try.o sock.o libcpglock.o
	gcc -o $@ -L. $^  -lpthread

check: locktest
	LD_LIBRARY_PATH=. ./locktest test

install: libcpglock.so cpglockd
	install -m 0755 libcpglock.so /usr/lib64
	install -m 0755 cpglockd /usr/bin

%.o: %.c
	gcc -I. -c $^ -o $@ -fPIC $(CFLAGS)

clean:
	rm -f *.o *.d *~ libcpglock.so cpglockd locktest lock lockstress many thread thread2 try core.* *orig many2
