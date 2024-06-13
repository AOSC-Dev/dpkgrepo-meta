CFLAGS = -fPIC -O2 -Wall -Wextra

all: mod_vercomp.so

mod_vercomp.so: vercomp.c
	$(CC) $(CFLAGS) -shared vercomp.c -o mod_vercomp.so

clean:
	-rm -f mod_vercomp.so
