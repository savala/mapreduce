#include <boost/mpi.hpp>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <map>
#include <utility>

namespace mpi = boost::mpi;

using namespace std;

template<typename Ftype, typename Stype>
class tuple {
    private:
        friend class boost::serialization::access;

        template<class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar &_first;
            ar &_second;
        }
    
    public:
        Ftype _first;
        Stype _second;

        tuple() { }
        
        tuple(Ftype f, Stype s) :
            _first(f), 
            _second(s)
        { }

};

int main() {
    
    mpi::environment env;
    mpi::communicator world;

    if (world.rank() == 0) {
        vector<int> s;
        s.push_back(4);
        s.push_back(3);
        tuple<int, vector<int> > p(2, s);

        world.send(1, 0, p);
    }
    if (world.rank() == 1) {
        tuple<int, vector<int> > q;
        world.recv(0, 0, q);
        printf("hahaha %d:%d\n", q._first, q._second[0]);
    }
    printf("ME: %d / %d\n", world.rank(), world.size());
    return 0;
}
