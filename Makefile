mapreduce: mapreduce.h mapreduce.cxx
	mpicxx -g -O2 -c mapreduce.cxx

example: mapreduce.h mapreduce.cxx example.cxx
	mpicxx -g -O2 -c example.cxx
	mpicxx -g -O2 example.o -o example.out