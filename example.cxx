# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

class CharCountMaster : public Master {
    public:

    void initialize() {
        _map_container[0] = spair<char, int>('a', 1);
        _map_container[1] = spair<char, int>('a', 1);
        _map_container[2] = spair<char, int>('b', 1);
        _map_container[3] = spair<char, int>('c', 1);
    }

    void finalize() const {
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%c:%d ", _result_container[i].first, _result_container[i].second);
        }
    }
};

class CharCountMapper : public Mapper {

    vector<spair<char, int> > map(vector<spair<char, int> > spairs) {
        return spairs;
    }
};

class CharCountReducer : public Reducer {

    spair<char, int> reduce(char key, vector<int> values) {
        int sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            sum += values[i];
        }
        return spair<char, int>(key, sum);
    }
};

int main() {
    JobClient jc;
    CharCountMaster master;
    CharCountMapper mapper;
    CharCountReducer reducer;

    master.initialize();
    jc.run(master, mapper, reducer);
    master.finalize();
}