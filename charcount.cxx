# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

class CharCountMaster : public Master<char, int, char, int> {

    virtual void initialize() {
        vector<tuple<char, int> > v;
        v.push_back(tuple<char, int>('a', 1));
        v.push_back(tuple<char, int>('a', 1));
        _map_container.push_back(v);
        v.clear();
        v.push_back(tuple<char, int>('b', 1));
        v.push_back(tuple<char, int>('c', 1));
        _map_container.push_back(v);
        v.clear();
        v.push_back(tuple<char, int>('a', 1));
        v.push_back(tuple<char, int>('d', 1));
        _map_container.push_back(v);
    }

    virtual void finalize() const {
        printf("Char counts: \n");
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%c:%d ", _result_container[i].first, _result_container[i].second);
        }
        printf("\n");
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
    Master<char, int, char, int>* master = new CharCountMaster();
    Mapper<char, int, char, int>* mapper = new CharCountMapper();
    Reducer<char, int>* reducer = new CharCountReducer();

    jc.run(master, mapper, reducer);
    delete master;
    delete mapper;
    delete reducer;
}