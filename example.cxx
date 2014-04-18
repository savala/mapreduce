# include "mapreduce.h"

class WordCount {

    class WordCountMap<K, V>: public Mapper {
        map() {

        }
    };

    class WordCountReduce: public Reducer {
        reduce() {

        }
    };

};

int main() {
    int data[] = [1, 2, 3, 4];
    int[] part = map(data, function); // MPI_Scatter
    int result = reduce(par, function); // MPI_Reduce
}

// a = [1,2,3];
// a.map(x => x+1).reduce(x,y => x*y);