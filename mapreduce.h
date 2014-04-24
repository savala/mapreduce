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
class tuple {
    private:
        friend class boost::serialization::access;

        template<class Archive>
        void serialize(Archive &ar, const unsigned int version) {
            ar &first;
            ar &second;
        }
    
    public:
        Ftype first;
        Stype second;
        
        tuple() { }
        
        tuple(Ftype f, Stype s) :
            first(f), 
            second(s)
        { }
};

template<typename MK, typename MV, typename RK, typename RV>
class Master {
    protected:
        vector<tuple<MK, MV> > _map_container;
        vector<tuple<RK, RV> > _result_container;
        map  <RK, vector<RV> > _reduce_container;
        int _free_processor;
    
    public:
        Master() :
            _map_container   (vector<tuple<MK, MV> >()),
            _result_container(vector<tuple<RK, RV> >()),
            _reduce_container(map  <RK, vector<RV> >()),
            _free_processor  (1)
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
                    vector<tuple<RK, RV> > contribution;

                    for (int i = 1; i < size; ++i) {
                        world.recv(i, 0, contribution);

                        for (int j = 0; j < contribution.size(); ++j) {
                            if(_reduce_container.count(contribution[j].first) == 0) {
                                _reduce_container[contribution[j].first] = vector<RV>();
                            }
                            _reduce_container[contribution[j].first].push_back(contribution[j].second);
                        }
                    }
                    _free_processor = 1;
                }
            }

            // MAP CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                vector<tuple<RK, RV> > contribution;

                world.recv(i, 0, contribution);
                for (int j = 0; j < contribution.size(); ++j) {
                    if(_reduce_container.count(contribution[j].first) == 0) {
                        _reduce_container[contribution[j].first] = vector<RV>();
                    }
                    _reduce_container[contribution[j].first].push_back(contribution[j].second);
                }
            }

            typedef map<char, vector<int> >::iterator it_type;
            it_type iterator = _reduce_container.begin();
            _free_processor = 1;
            // REDUCE WORK
            while(iterator != _reduce_container.end()) {
                tuple<RK, vector<RV> > work;
                work.first = iterator->first;
                work.second = iterator->second;

                world.send(_free_processor, 0, REDUCER);
                world.send(_free_processor, 0, work);

                _free_processor++;
                iterator++;
                if(_free_processor == size) {
                    tuple<RK, RV> contribution;

                    for (int i = 1; i < size; ++i) {
                        world.recv(i, 0, contribution);
                        _result_container.push_back(contribution);
                    }
                    _free_processor = 1;
                }
            }

            // REDUCE CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                tuple<RK, RV> contribution;

                world.recv(i, 0, contribution);
                _result_container.push_back(contribution);
            }
        }

        virtual ~Master() { }
};

template<typename MK, typename MV, typename RK, typename RV>
class Mapper {
    public:    
        Mapper() { }

        virtual vector<tuple<RK, RV> > map(vector<tuple<MK, MV> > tuples) = 0;

        void work() {
            mpi::communicator world;
            int err;

            vector<tuple<MK, MV> > tuples;
            vector<tuple<RK, RV> > results;
            world.recv(ROOT, 0, tuples);
            results = map(tuples);
            world.send(ROOT, 0, results);
        }
        
        virtual ~Mapper() { }
};

template<typename RK, typename RV>
class Reducer {
    public:      
        Reducer() { }

        virtual tuple<RK, RV> reduce(RK key, vector<RV> values) = 0;

        void work() {
            mpi::communicator world;
            int err;

            tuple<RK, vector<RV> > tuples;
            tuple<RK, RV> result;
            world.recv(ROOT, 0, tuples);
            result = reduce(tuples.first, tuples.second);
            world.send(ROOT, 0, result);
        }

        virtual ~Reducer() { }
};

template<typename MK, typename MV, typename RK, typename RV>
class JobClient {
    public:
        
        void run(Master<MK, MV, RK, RV> master, Mapper<MK, MV, RK, RV> mapper, Reducer<RK, RV> reducer) {
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

        void wait_for_work(Mapper<MK, MV, RK, RV> mapper, Reducer<RK, RV> reducer) {
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