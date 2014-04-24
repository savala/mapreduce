example: mapreduce.h example.cxx
	mpicxx -g -O2 example.cxx /usr/local/opt/boost/lib/libboost_mpi-mt.a /usr/local/opt/boost/lib/libboost_serialization-mt.a -o example.out

hello: hello.cxx
	mpicxx -g -O2 hello.cxx /usr/local/opt/boost/lib/libboost_mpi-mt.a /usr/local/opt/boost/lib/libboost_serialization-mt.a

clean:
	/bin/rm -rf *.o *.out *.dSYM