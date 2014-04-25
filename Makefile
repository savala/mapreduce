MPI_LIB_PATH=/usr/local/opt/boost/lib

count: mapreduce.h count.cxx
	mpicxx -g -O2 count.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o count.out

hello: hello.cxx
	mpicxx -g -O2 hello.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o hello.out

matrix: matrix.cxx
	mpicxx -g -O2 matrix.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o matrix.out

clean:
	/bin/rm -rf *.o *.out *.dSYM