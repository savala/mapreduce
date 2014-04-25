MPI_LIB_PATH=/usr/local/opt/boost/lib

charcount: mapreduce.h charcount.cxx
	mpicxx -g -O2 charcount.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o charcount.out

hello: hello.cxx
	mpicxx -g -O2 hello.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o hello.out

matrix: matrix.cxx
	mpicxx -g -O2 matrix.cxx -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o matrix.out

clean:
	/bin/rm -rf *.o *.out *.dSYM