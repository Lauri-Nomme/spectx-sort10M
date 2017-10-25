sort10M: sort10M.c
	gcc -std=gnu11 -pthread -O3 -o sort10M sort10M.c

test: sort10M
	rm -f out.txt && time ./sort10M numbers.txt out.txt && echo '41398970ebd7cb3aa8c38c35bb5b4339  out.txt' | md5sum -c -
