# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

class MatrixMaster : public Master<int, vector<tuple<int, int> >, int, vector<int> > {

    virtual void initialize() {        
        vector<tuple<int, vector<tuple<int, int> > > > list;
        tuple<int, vector<tuple<int, int> > > listTuple;
        
        listTuple.first = 1;
        vector<tuple<int, int> > tupleValues;
        tupleValues.push_back(tuple<int, int>(7, 5));
        tupleValues.push_back(tuple<int, int>(4, 1));
        tupleValues.push_back(tuple<int, int>(2, 7));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);

        list.clear();

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(tuple<int, int>(6, 5));
        listTuple.second = tupleValues;

        list.push_back(listTuple);

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(tuple<int, int>(3, 1));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);

        list.clear();

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(tuple<int, int>(9, 7));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);
    }

    virtual void finalize() const {
        printf("Result: \n");
        // vector<tuple<RK, RV> > _result_container;
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%d:%d ", _result_container[i].first, _result_container[i].second[0]);
        }
        printf("\n");
    }
};

class MatrixMapper : public Mapper<int, vector<tuple<int, int> >, int, vector<int> > {

    // vector<tuple<RK, RV> > map(vector<tuple<MK, MV> > tuples)
    vector<tuple<int, vector<int> > > map(vector<tuple<int, vector<tuple<int, int> > > > tuples) {
        vector<tuple<int, vector<int> > > result;
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

    // tuple<RK, RV> reduce(RK key, vector<RV> values)
    tuple<int, vector<int> > reduce(int key, vector<vector<int> > values) {
        int sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            vector<int> list = values[i];

            for (int j = 0; j < list.size(); ++j) {
                sum += list[j];                
            }
        }

        vector<int> result;
        result.push_back(sum);
        return tuple<int, vector<int> >(key, result);
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