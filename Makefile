example: mapreduce.h example.cxx
	mpicxx -g -O2 example.cxx /usr/local/opt/boost/lib/libboost_mpi-mt.a

hello: hello.cxx
	mpicxx -g -O2 hello.cxx /usr/local/opt/boost/lib/libboost_mpi-mt.a