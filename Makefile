example: mapreduce.h example.cxx
	mpicxx -g -O2 -c example.cxx
	mpicxx -g -O2 example.o -o example.out