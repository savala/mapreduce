#ifndef Mapreduce_h
#define Mapreduce_h

#include <vector>
#include <map>
#include <utility>
#include <boost/mpi.hpp>

#define ROOT 0

enum JobType { MAPPER = 0, REDUCER = 1, DONE = 2 };

using namespace std;
namespace mpi = boost::mpi;

template<typename Ftype, typename Stype>
class spair : public pair<Ftype, Stype> {
    private:
        friend class boost::serialization::access;

        template<class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar &_first;
            ar &_second;
        }

        Ftype _first;
        Stype _second;
    
    public:
        spair() { }
        
        spair(Ftype f, Stype s) :
            _first(f), 
            _second(s),
            pair<Ftype, Stype>(make_pair<Ftype, Stype>(f, s))
        { }
};

class Master {    
    protected:
        vector<spair<char, int> > _map_container;
        vector<spair<char, int> > _result_container;
        map< char, vector<int> > _reduce_container;
        int _free_processor;
    
    public:
        Master() :
            _map_container(vector<spair<char, int> >()),
            _reduce_container(map<char, vector<int> >()),
            _result_container(vector<spair<char, int> >()),
            _free_processor(1)
        { }
        
        virtual void initialize() = 0;

        virtual void finalize() const = 0;

        void run() {
            mpi::communicator world;

            int size = world.size();
            int task = 0;
            int msize = _map_container.size();
            
            // MAP WORK
            while(task < msize) {

                world.send(_free_processor, 0, MAPPER);
                world.send(_free_processor, 0, _map_container[task]);
                
                task++;
                _free_processor++;
                if(_free_processor == size) {
                    vector<spair<char, int> > contribution;

                    for (int i = 1; i < size; ++i) {
                        world.recv(i, 0, contribution);

                        for (int j = 0; j < contribution.size(); ++j) {
                            if(_reduce_container.count(contribution[j].first) == 0) {
                                _reduce_container[contribution[j].first] = vector<int>();
                            }
                            _reduce_container[contribution[j].first].push_back(contribution[j].second);
                        }
                    }
                    _free_processor = 1;
                }
            }

            // MAP CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                vector<spair<char, int> > contribution;

                world.recv(i, 0, contribution);
                for (int j = 0; j < contribution.size(); ++j) {
                    if(_reduce_container.count(contribution[j].first) == 0) {
                        _reduce_container[contribution[j].first] = vector<int>();
                    }
                    _reduce_container[contribution[j].first].push_back(contribution[j].second);
                }
            }

            typedef map<char, vector<int> >::iterator it_type;
            it_type iterator = _reduce_container.begin();
            _free_processor = 1;
            task = 0;
            // REDUCE WORK
            while(iterator != _reduce_container.end()) {
                spair<char, vector<int> > work;
                work.first = iterator->first;
                work.second = iterator->second;

                world.send(_free_processor, 0, REDUCER);
                world.send(_free_processor, 0, work);

                _free_processor++;
                iterator++;
                if(_free_processor == size) {
                    spair<char, int> contribution;

                    for (int i = 1; i < size; ++i) {
                        world.recv(i, 0, contribution);
                        _result_container.push_back(contribution);
                    }
                    _free_processor = 1;
                }
            }

            // REDUCE CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                spair<char, int> contribution;

                world.recv(i, 0, contribution);
                _result_container.push_back(contribution);
            }
        }

        virtual ~Master() { }
};

class Mapper {
    public:    
        Mapper() { }

        virtual vector<spair<char, int> > map(vector<spair<char, int> > spairs) = 0;

        void work() {
            mpi::communicator world;
            int err;

            vector<spair<char, int> > spairs;
            vector<spair<char, int> > results;
            world.recv(ROOT, 0, spairs);
            results = map(spairs);
            world.send(ROOT, 0, results);
        }
        
        virtual ~Mapper() { }
};

class Reducer {
    public:        
        Reducer() { }

        virtual spair<char, int> reduce(char key, vector<int> values) = 0;

        void work() {
            mpi::communicator world;
            int err;

            spair<char, vector<int> > spairs;
            spair<char, int> result;
            world.recv(ROOT, 0, spairs);
            result = reduce(spairs.first, spairs.second);
            world.send(ROOT, 0, result);
        }

        virtual ~Reducer() { }
};

class JobClient {
    public:     
        void run(Master master, Mapper mapper, Reducer reducer) {
            mpi::environment env;
            mpi::communicator world;
            int rank = world.rank();
            int size = world.size();
            
            if (rank == ROOT) {
                master.run();
            } else {
                wait_for_work(mapper, reducer);
            }
        }

        void wait_for_work(Mapper mapper, Reducer reducer) {
            mpi::communicator world;

            bool done = false;
            while(!done) {
                int err;
                MPI_Status s;
                JobType w;

                world.recv(ROOT, 0, w);
                switch(w) {
                    case MAPPER     : mapper.work();  break;
                    case REDUCER    : reducer.work(); break;
                    case DONE       : done = true;    break;
                    default         : cout << "This should not happen." << endl;
                }
            }
        }
};

#endif // Mapreduce_h