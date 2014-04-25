#include <mapreduce.h>

#include <vector>
#include <utility>

typedef tuple<int, int> VL;

typedef int             MK;
typedef vector<VL>      MV;
typedef int             RK;
typedef vector<int>     RV;

class MatrixMaster : public Master<MK, MV, RK, RV> {

    virtual void initialize() {        
        vector<MPAIR> v;
        MPAIR mp;
        
        mp.first = 1;
        MV mv;
        mv.push_back(VL(7, 5));
        mv.push_back(VL(4, 1));
        mv.push_back(VL(2, 7));
        mp.second = mv;
        v.push_back(mp);
        
        _map_container.push_back(v);
        v.clear();

        mp.first = 2;
        mv.clear();
        mv.push_back(VL(6, 5));
        mp.second = mv;
        v.push_back(mp);

        mp.first = 2;
        mv.clear();
        mv.push_back(VL(3, 1));
        mp.second = mv;
        v.push_back(mp);

        _map_container.push_back(v);
        v.clear();

        mp.first = 2;
        mv.clear();
        mv.push_back(VL(9, 7));
        mp.second = mv;
        v.push_back(mp);

        _map_container.push_back(v);
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

    virtual vector<RPAIR> map(vector<MPAIR> tuples) {
        vector<RPAIR> result;
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
            RV rv = values[i];

            for (int j = 0; j < rv.size(); ++j) {
                sum += rv[j];                
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