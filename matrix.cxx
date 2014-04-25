# include "mapreduce.h"

#include <vector>
#include <map>
#include <utility>

typedef tuple<int, int> VL;

typedef int             MK;
typedef vector<VL>      MV;
typedef int             RK;
typedef vector<int>     RV;

typedef tuple<MK, MV>   MPAIR;
typedef tuple<RK, RV>   RPAIR;

class MatrixMaster : public Master<MK, MV, RK, RV> {

    virtual void initialize() {        
        vector<MPAIR> list;
        MPAIR listTuple;
        
        listTuple.first = 1;
        MV tupleValues;
        tupleValues.push_back(VL(7, 5));
        tupleValues.push_back(VL(4, 1));
        tupleValues.push_back(VL(2, 7));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);

        list.clear();

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(VL(6, 5));
        listTuple.second = tupleValues;

        list.push_back(listTuple);

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(VL(3, 1));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);

        list.clear();

        listTuple.first = 2;
        tupleValues.clear();
        tupleValues.push_back(VL(9, 7));
        listTuple.second = tupleValues;

        list.push_back(listTuple);
        _map_container.push_back(list);
    }

    virtual void finalize() const {
        printf("Result: \n");
        
        for (int i = 0; i < _result_container.size(); ++i) {
            printf("%d:%d ", _result_container[i].first, _result_container[i].second[0]);
        }
        printf("\n");
    }
};

class MatrixMapper : public Mapper<MK, MV, RK, RV> {

    virtual vector<RPAIR > map(vector<MPAIR> tuples) {
        vector<RPAIR > result;
        for (int i = 0; i < tuples.size(); ++i) {
            RK key = tuples[i].first;
            MV values = tuples[i].second;

            RV prods;
            for (int j = 0; j < values.size(); ++j) {
                int r = values[j].first;
                int c = values[j].second;

                prods.push_back(r * c);
            }

            result.push_back(RPAIR(key, prods));
        }

        return result;
    }
};

class MatrixReducer : public Reducer<RK, RV> {

    virtual RPAIR reduce(RK key, vector<RV> values) {
        int sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            RV list = values[i];

            for (int j = 0; j < list.size(); ++j) {
                sum += list[j];                
            }
        }

        RV result;
        result.push_back(sum);
        return RPAIR(key, result);
    }
};

int main() {
    JobClient<MK, MV, RK, RV>   jc;
    Master   <MK, MV, RK, RV>*  master  = new MatrixMaster();
    Mapper   <MK, MV, RK, RV>*  mapper  = new MatrixMapper();
    Reducer  <RK, RV>*          reducer = new MatrixReducer();

    jc.run(master, mapper, reducer);
    delete master;
    delete mapper;
    delete reducer;
}