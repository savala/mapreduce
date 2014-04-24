example: mapreduce.h example.cxx
	mpicxx -g -O2 -c example.cxx /usr/local/opt/boost/lib/libboost_mpi-mt.a
	mpicxx -g -O2 example.o -o example.out