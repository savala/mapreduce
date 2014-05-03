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

        tuple(const tuple &that) :
            first(that.first),
            second(that.second)
        { }

        tuple& operator = (tuple that) {
            swap(first, that.first);
            swap(second, that.second);
            return *this;
        }
};

template<typename MK, typename MV, typename RK, typename RV>
class Master {
    
    private:
        int _free_processor;
        typedef typename map<RK, vector<RV> >::iterator it_type;

        void receive_map_results(int size) {
            mpi::communicator world;

            for (int i = 1; i < size; ++i) {
                vector<RPAIR> contribution;
                
                world.recv(i, 0, contribution);
                for (int j = 0; j < contribution.size(); ++j) {
                    RK key = contribution[j].first;
                    _reduce_container[key].push_back(contribution[j].second);
                    cout << "*";
                }
            }
        }

        void receive_reduce_results(int size) {
            mpi::communicator world;

            for (int i = 1; i < size; ++i) {
                RPAIR contribution;
                
                world.recv(i, 0, contribution);
                _result_container.push_back(contribution);
                cout << "@";
            }
        }

    protected:
        typedef tuple<MK, MV> MPAIR;
        typedef tuple<RK, RV> RPAIR;

        vector<vector<MPAIR>  > _map_container;
        map   <RK, vector<RV> > _reduce_container;
        vector<RPAIR>           _result_container;
        
        vector<mpi::request>    _requests;
    
    public:
        Master(int size) :
            _map_container   (vector<vector<MPAIR> >()),
            _result_container(vector<RPAIR>()),
            _reduce_container(map   <RK, vector<RV> >()),
            _requests        (size),
            _free_processor  (1)
        { }
        
        virtual void initialize() = 0;

        virtual void finalize() const = 0;

        void run() {
            mpi::communicator world;

            int size = world.size();
            int task = 0;
            int msize = _map_container.size();

            mpi::request *r = &_requests[0];

            // MAP WORK
            while(task < msize) {
                world.send(_free_processor, 0, MAPPER);
                r[_free_processor] = world.isend(_free_processor, 0, _map_container[task]);
                
                task++;
                _free_processor++;
                if(_free_processor == size) {
                    mpi::wait_all(r + 1, r + size);
                    receive_map_results(size);                    
                    _free_processor = 1;
                }
            }

            // MAP CLEAN UP
            mpi::wait_all(r + 1, r + _free_processor);
            receive_map_results(_free_processor);

            it_type it = _reduce_container.begin();
            _free_processor = 1;
            
            // REDUCE WORK
            while(it != _reduce_container.end()) {
                tuple<RK, vector<RV> > work;
                work.first = it->first;
                work.second = it->second;

                world.send(_free_processor, 0, REDUCER);
                r[_free_processor] = world.isend(_free_processor, 0, work);

                _free_processor++;
                it++;
                if(_free_processor == size) {
                    mpi::wait_all(r + 1, r + size);
                    receive_reduce_results(size);
                    _free_processor = 1;
                }
            }

            // REDUCE CLEAN UP
            mpi::wait_all(r + 1, r + _free_processor);
            receive_reduce_results(_free_processor);

            for (int i = 1; i < size; ++i) {
                world.send(i, 0, DONE);
            }
            cout << endl;
        }

        virtual ~Master() { }
};

template<typename MK, typename MV, typename RK, typename RV>
class Mapper {

    protected:
        typedef tuple<MK, MV> MPAIR;
        typedef tuple<RK, RV> RPAIR;
    
    public:    
        Mapper() { }

        virtual vector<RPAIR> map(vector<MPAIR> tuples) = 0;

        void work() {
            mpi::communicator world;

            vector<MPAIR> tuples;
            vector<RPAIR> results;
            world.recv(ROOT, 0, tuples);
            results = map(tuples);
            world.send(ROOT, 0, results);
        }
        
        virtual ~Mapper() { }
};

template<typename RK, typename RV>
class Reducer {

    protected:
        typedef tuple<RK, RV> RPAIR;

    public:      
        Reducer() { }

        virtual RPAIR reduce(RK key, vector<RV> values) = 0;

        void work() {
            mpi::communicator world;

            tuple<RK, vector<RV> > tuples;
            RPAIR result;
            world.recv(ROOT, 0, tuples);
            result = reduce(tuples.first, tuples.second);
            world.send(ROOT, 0, result);
        }

        virtual ~Reducer() { }
};

template<typename MK, typename MV, typename RK, typename RV>
class JobClient {
    
    public:
        void run(Master<MK, MV, RK, RV>* master, Mapper<MK, MV, RK, RV>* mapper, Reducer<RK, RV>* reducer) {
            mpi::environment env;
            mpi::communicator world;
            int rank = world.rank();
            int size = world.size();
            
            if (rank == ROOT) {
                cout << "Master " << rank << " with " << size-1 << " workers." << endl;
                master->initialize();
                master->run();
                master->finalize();
            } else {
                wait_for_work(mapper, reducer);
            }
        }

        void wait_for_work(Mapper<MK, MV, RK, RV>* mapper, Reducer<RK, RV>* reducer) {
            mpi::communicator world;

            bool done = false;
            while(!done) {
                JobType w;

                world.recv(ROOT, 0, w);
                switch(w) {
                    case MAPPER  : mapper->work();  break;
                    case REDUCER : reducer->work(); break;
                    case DONE    : done = true;     break;
                    default      : cout << "This should not happen." << endl;
                }
            }
        }
};

#endif // Mapreduce_h
