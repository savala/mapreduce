# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

class MatrixMaster : public Master<int, vector<tuple<int, int> >, int, vector<int> > {
    public:

    virtual void initialize() {

    }

    virtual void finalize() const {
        printf("Char counts: \n");
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%c:%d ", _result_container[i].first, _result_container[i].second);
        }
        printf("\n");
    }
};

class MatrixMapper : public Mapper<int, vector<tuple<int, int> >, int, vector<int> > {

    vector<tuple<int, vector<int>>> map(vector<tuple<int, vector<tuple<int, int>>>> tuples) {
        vector<tuple<int, vector<int>>> result;
        for (int i = 0; i < tuples.size(); ++i) {
            int key = tuples[i].first;
            vector<tuple<int, int> > values = tuples[i].second;

            vector<int> prods;
            for (int j = 0; j < values.size(); ++j) {
                int r = values[j].first;
                int c = values[j].second;

                prods.push_back(r * c);
            }

            result.push_back(tuple<int, vector<int> >(key, prods));
        }

        return result;
    }
};

class MatrixReducer : public Reducer<int, vector<int> > {

    tuple<int, int> reduce(int key, vector<int> values) {
        int sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            sum += values[i];
        }
        return tuple<int, int>(key, sum);
    }
};

int main() {
    JobClient<int, vector<tuple<int, int> >, int, vector<int> > jc;
    Master<int, vector<tuple<int, int> >, int, vector<int> >* master = new MatrixMaster();
    Mapper<int, vector<tuple<int, int> >, int, vector<int> >* mapper = new MatrixMapper();
    Reducer<int, vector<int> >* reducer = new MatrixReducer();

    jc.run(master, mapper, reducer);
    delete master;
    delete mapper;
    delete reducer;
}