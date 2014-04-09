#ifndef Mapreduce_h
#define Mapreduce_h

#include <vector>
#include <map>
#include <utility>
#include "mpi.h"

#define ROOT 0

enum JobType { MAPPER = 0, REDUCER = 1, DONE = 2 };

using namespace std;

template<typename K1, typename V1, typename K2, typename V2>
class Master {
    private:
        vector<pair<K1, V1> > _map_container;
        map<K2,  vector<V2> > _reduce_container;
        vector<pair<K2, V2> > _result_container;
        int _free_processor;
    
    public:
        Master() :
            _map_container(vector<pair<K1, V1> >()),
            _reduce_container(map<K2,  vector<V2> >()),
            _result_container(vector<pair<K2, V2> >()),
            _free_processor(1)
        { }
        
        virtual void initialize();

        virtual void finalize() const;

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
                err = MPI_Send(&_map_container[task], 1, MPI_INT, _free_processor, 0, MPI_COMM_WORLD);
                
                task++;
                _free_processor++;
                if(_free_processor == size) {
                    MPI_Status status;
                    vector<pair<K2, V2> > contribution;

                    for (int i = 1; i < size; ++i) {
                        err = MPI_Recv(&contribution, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);

                        for (int j = 0; j < contribution.size(); ++j) {
                            if(_reduce_container.count(contribution[j][0]) == 0) {
                                _reduce_container[contribution[j][0]](vector<V2>());
                            }
                            _reduce_container[contribution[j][0]].append(contribution[j][1]);
                        }
                    }
                    _free_processor = 1;
                }
            }

            // MAP CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                MPI_Status status;
                vector<pair<K2, V2> > contribution;

                for (int j = 0; j < contribution.size(); ++j) {
                    if(_reduce_container.count(contribution[j][0]) == 0) {
                        _reduce_container[contribution[j][0]](vector<V2>());
                    }
                    _reduce_container[contribution[j][0]].append(contribution[j][1]);
                }
            }

            _free_processor = 1;
            task = 0;
            int rsize = _reduce_container.size();
            // REDUCE WORK
            while(task < rsize) {
                pair<K2, V2> work;
                work[0](_reduce_container[task][0]);
                work[1](_reduce_container[task][1]);

                err = MPI_Send(REDUCER, 1, MPI_INT, _free_processor, 0, MPI_COMM_WORLD);
                err = MPI_Send(&work, 1, MPI_INT, _free_processor, 0, MPI_COMM_WORLD);

                task++;
                _free_processor++;
                if(_free_processor == size) {
                    MPI_Status status;
                    pair<K2, V2> contribution;

                    for (int i = 1; i < size; ++i) {
                        err = MPI_Recv(&contribution, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
                        _result_container.append(contribution);
                    }
                    _free_processor = 1;
                }
            }

            // REDUCE CLEAN UP
            for (int i = 1; i < _free_processor; ++i) {
                MPI_Status status;
                pair<K2, V2> contribution;

                err = MPI_Recv(&contribution, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
                _result_container.append(contribution);
            }
        }

        virtual ~Master();
};

template<typename K1, typename V1, typename K2, typename V2>
class Mapper {
    private:       

    public:    
        Mapper();

        virtual vector<pair<K2, V2> > map(vector<pair<K1, V1> > pairs);

        void work() {
            int rank, size;
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);

            int err;
            MPI_Status status;
            vector<pair<K1, V1> > pairs;
            vector<pair<K2, V2> > results;
            err = MPI_Recv(&pairs, 1, MPI_INT, ROOT, 0, MPI_COMM_WORLD, &status);
            results = map(pairs);
            err = MPI_Send(&results, 1, MPI_INT, ROOT, 0, MPI_COMM_WORLD);
        }
        
        virtual ~Mapper();
};

template<typename K2, typename V2>
class Reducer {
    private:

    public:      
        Reducer();

        virtual pair<K2, V2> reduce(K2 key, vector<V2> values);

        void work() {
            int rank, size;
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);

            int err;
            MPI_Status status;
            pair<K2, vector<V2> > pairs;
            pair<K2, V2> result;
            err = MPI_Recv(&pairs, 1, MPI_INT, ROOT, 0, MPI_COMM_WORLD, &status);
            result = reduce(pairs);
            err = MPI_Send(&result, 1, MPI_INT, ROOT, 0, MPI_COMM_WORLD);
        }

        virtual ~Reducer();
};

template<typename K1, typename V1, typename K2, typename V2>
class JobClient {
    public:
        
        void run(Master<K1, V1, K2, V2> m, Mapper<K1, V1, K2, V2> t, Reducer<K2, V2> r) {
            MPI_Init(NULL, NULL);
            int rank, size;
            MPI_Comm_rank(MPI_COMM_WORLD, &rank);
            MPI_Comm_size(MPI_COMM_WORLD, &size);
            
            if (rank == ROOT) {
                m.run();
            } else {
                wait_for_work(t, r);
            }
        }

        void wait_for_work(Mapper<K1, V1, K2, V2> m, Reducer<K2, V2> r) {
            bool done = false;
            while(!done) {
                int err;
                MPI_Status s;
                JobType w;

                err = MPI_Recv(&w, 1, MPI_INT, ROOT, 0, MPI_COMM_WORLD, &s);
                switch(w) {
                    case MAPPER     : m.work();    break;
                    case REDUCER    : r.work();    break;
                    case DONE       : done = true; break;
                    default         : cout << "This should not happen." << endl;
                }
            }
        }

};

#endif // Mapreduce_h