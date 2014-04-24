# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

class CharCountMaster : public Master<char, int, char, int> {
    public:

    void initialize() {
        _map_container[0] = tuple<char, int>('a', 1);
        _map_container[1] = tuple<char, int>('a', 1);
        _map_container[2] = tuple<char, int>('b', 1);
        _map_container[3] = tuple<char, int>('c', 1);
    }

    void finalize() const {
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%c:%d ", _result_container[i].first, _result_container[i].second);
        }
    }
};

class CharCountMapper : public Mapper<char, int, char, int> {

    vector<tuple<char, int> > map(vector<tuple<char, int> > tuples) {
        return tuples;
    }
};

class CharCountReducer : public Reducer<char, int> {

    tuple<char, int> reduce(char key, vector<int> values) {
        int sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            sum += values[i];
        }
        return tuple<char, int>(key, sum);
    }
};

int main() {
    JobClient<char, int, char, int> jc;
    CharCountMaster master;
    CharCountMapper mapper;
    CharCountReducer reducer;

    master.initialize();
    jc.run(master, mapper, reducer);
    master.finalize();
}