#ifndef Mapreduce_h
#define Mapreduce_h

#include <vector>
#include <map>
#include <utility>
#include "mpi.h"

#define MPI_ROOT 0

enum JobType { MAPPER = 0, REDUCER = 1, DONE = 2 };

using namespace std;

template<K1, V1, K2, V2>
class Master : public Handler {
    private:
        vector<pair<K1, V1> > _map_container;
        map<K2,  vector<V2> > _reduce_container;
        vector<pair<K2, V2> > _result_container;
        int _free_processor;
    
    public:
        Master() :
            _map_container(vector<pair<K1, V1> >()),
            _reduce_container(map<K2,  vector<V2> >()),
            _result_container(vector<pair<K2, V2>()),
            _free_processor(1)
        { }
        
        virtual initialize();

        virtual finalize() const;

        void run() {
            MPI_Init(NULL, NULL);
            int rank, size, err;
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);

            int task = 0;
            int msize = _map_container.size();
            // MAP WORK
            while(task < msize) {

                err = MPI_Send(MAPPER, 1, MPI_INT, _free_processor, 0, MPI_COMM_WORLD);
                err = MPI_Send(&_map_container[task], 1, , _free_processor, 0, MPI_COMM_WORLD);
                
                task++;
                _free_processor++;
                if(_free_processor == size) {
                    MPI_Status status;
                    pair<K2, V2> contribution;

                    for (int i = 1; i < size; ++i) {
                        err = MPI_Recv(&contribution, 1, , i, 0, MPI_COMM_WORLD, &status);
                        if(_reduce_container.count(contribution[0]) == 0) {
                            _reduce_container[contribution[0]]();
                        }
                        _reduce_container[contribution[0]].append(contribution[1]);
                    }
                    _free_processor = 1;
                }
            }

            // MAP CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                MPI_Status status;
                pair<K2, V2> contribution;

                if(_reduce_container.count(contribution[0]) == 0) {
                    _reduce_container[contribution[0]]();
                }
                _reduce_container[contribution[0]].append(contribution[1]);
            }

            _free_processor = 1;
            task = 0;
            int rsize = _reduce_container.size();
            // REDUCE WORK
            while(task < rsize) {

                err = MPI_Send(REDUCER, 1, MPI_INT, _free_processor, 0, MPI_COMM_WORLD);
                err = MPI_Send(&_reduce_container[task], 1, ,_free_processor, 0, MPI_COMM_WORLD);

                task++;
                _free_processor++;
                if(_free_processor == size) {
                    MPI_Status status;
                    pair<K2, V2> contribution;

                    for (int i = 1; i < size; ++i) {
                        err = MPI_Recv(&contribution, 1, , i, 0, MPI_COMM_WORLD, &status);
                        _result_container.append(contribution);
                    }
                    _free_processor = 1;
                }
            }

            // REDUCE CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                MPI_Status status;
                pair<K2, V2> contribution;

                err = MPI_Recv(&contribution. 1, , i, 0, MPI_COMM_WORLD, &status);
                _result_container.append(contribution);
            }
        }

        virtual ~Master();
};

class Worker : public Handler {
    public:
        Worker();

        virtual ~Worker();

};

template<K1, V1, K2, V2>
class Mapper : public Worker {
    private:       

    public:    
        Mapper();

        virtual vector<pair<K2, V2> > map(vector<pair<K1, V1> > pairs);
        
        virtual ~Mapper();
};

template<K2, V2>
class Reducer : public Worker {
    private:

    public:      
        Reducer();

        virtual vector<V2> reduce(vector<pair<K2, V2> > pairs);

        virtual ~Reducer();
};

class JobClient {
    public:
        
        void run(Master m, Mapper t, Reducer r) {
            MPI_Init(NULL, NULL);
            int rank, size;
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
            
            if (rank == MPI_ROOT) {
                m.run();
            } else {
                wait_for_work(t, r);
            }
        }

        void wait_for_work(Mapper m, Reducer r) {
            bool done = false;
            while(!done) {
                int err;
                MPI_Status s;
                JobType w;

                err = MPI_Recv(&w, 1, MPI_INT, MPI_ROOT, 0, MPI_COMM_WORLD, &s);
                switch(w) {
                    case MAPPER     : m.map();     break;
                    case REDUCER    : r.reduce();  break;
                    case DONE       : done = true; break;
                    default         : cout << "This should not happen." << endl;
                }
            }
        }

};

#endif // Mapreduce_h