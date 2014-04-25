MPI_LIB_PATH=/usr/local/opt/boost/lib

%: mapreduce.h ./examples/%.cxx
	mpicxx -g -O2 ./examples/$*.cxx -I. -L${MPI_LIB_PATH} -lboost_mpi-mt -lboost_serialization-mt -o $@.out

clean:
	/bin/rm -rf *.o *.out *.dSYM