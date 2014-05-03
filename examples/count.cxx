#include <mapreduce.h>

#include <vector>
#include <utility>
#include <stdio.h>

typedef char MK;
typedef int  MV;
typedef char RK;
typedef int  RV;

class CharCountMaster : public Master<MK, MV, RK, RV> {

    public:
        CharCountMaster(int size):
            Master<MK, MV, RK, RV>(size)
        { }

        virtual void initialize() {
            vector<MPAIR> v;
            
            v.push_back(MPAIR('a', 1));
            v.push_back(MPAIR('a', 1));
            _map_container.push_back(v);
            v.clear();
            
            v.push_back(MPAIR('b', 1));
            v.push_back(MPAIR('c', 1));
            _map_container.push_back(v);
            v.clear();
            
            v.push_back(MPAIR('a', 1));
            v.push_back(MPAIR('d', 1));
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

class CharCountMapper : public Mapper<MK, MV, RK, RV> {

    virtual vector<RPAIR> map(vector<MPAIR> tuples) {
        return tuples;
    }
};

class CharCountReducer : public Reducer<RK, RV> {

    virtual RPAIR reduce(RK key, vector<RV> values) {
        RV sum = 0;
        for (int i = 0; i < values.size(); ++i) {
            sum += values[i];
        }
        return RPAIR(key, sum);
    }
};

int main() {
    JobClient<MK, MV, RK, RV>  jc;
    Master   <MK, MV, RK, RV>* master  = new CharCountMaster(16);
    Mapper   <MK, MV, RK, RV>* mapper  = new CharCountMapper();
    Reducer  <RK, RV>*         reducer = new CharCountReducer();

    jc.run(master, mapper, reducer);
    delete master;
    delete mapper;
    delete reducer;
}
