BOOST_PATH := /work/00434/eijkhout/boost/installation

BOOST_LIB_PATH := ${BOOST_PATH}/lib
BOOST_INC_PATH := ${BOOST_PATH}/include

%: mapreduce.h ./examples/%.cxx
    mpicxx -g -O2 -I${BOOST_INC_PATH} ./examples/$*.cxx -I. -L${BOOST_LIB_PATH} -lboost_mpi -lboost_serialization -o $@.out

clean:
	/bin/rm -rf *.o *.out *.dSYM
	