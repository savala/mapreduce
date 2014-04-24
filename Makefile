MPI_LIB_PATH=/usr/local/opt/boost/lib

example: mapreduce.h example.cxx
	mpicxx -g -O2 example.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o example.out

hello: hello.cxx
	mpicxx -g -O2 hello.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o hello.out

clean:
	/bin/rm -rf *.o *.out *.dSYM