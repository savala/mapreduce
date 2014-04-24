#include <boost/mpi.hpp>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <map>
#include <utility>

namespace mpi = boost::mpi;

using namespace std;

class spair : public pair<int, int> {
    public:
        int _first;
        int _second;

        spair() { }
        
        spair(int f, int s) :
            _first(f), 
            _second(s),
            pair<int, int>(make_pair<int, int>(f, s))
        { }
};

namespace boost {
    namespace serialization {
        
        template<class Archive>
        void serialize(Archive &ar, spair &g, const unsigned int version) {
            ar &g._first;
            ar &g._second;
        }
    }
}

int main() {
    
    mpi::environment env;
    mpi::communicator world;

    if (world.rank() == 0) {
        spair p(2, 2);

        world.send(1, 0, p);
    }
    if (world.rank() == 1) {
        spair q;
        world.recv(0, 0, q);
        printf("hahaha %d\n", q._first);
    }
    printf("ME: %d / %d\n", world.rank(), world.size());
    return 0;
}
