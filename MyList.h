/* ---------------------------------------------------------------
Práctica 1.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */

#ifndef MYLIST_H_
#define MYLIST_H_

#include <vector>
#include <pthread.h>

using namespace std;
	
template<class T>
class MyList
{
	pthread_rwlock_t 	rwlock = PTHREAD_RWLOCK_INITIALIZER;
	std::vector<T> 		List;
	
	public:
		MyQueue();
		
		inline void add (T &val) { 
			pthread_rwlock_wrlock(&rwlock);
			List.push_back(val); 
			pthread_rwlock_unlock(&rwlock);
		};

        inline int size() {
            pthread_rwlock_rdlock(&rwlock);
            int size = (int)List.size();
            pthread_rwlock_unlock(&rwlock);
            return size;
        }

		};
		template <class Container> auto begin (Container& cont) -> decltype (cont.begin());
		template <class Container> auto end (Container& cont) -> decltype (cont.end());
		inline T& front() {
			if (!Queue.empty()){
				//pthread_rwlock_rdlock(&rwlock);
				return Queue.front(); 
				//pthread_rwlock_unlock(&rwlock);
			}
		} ;
		inline bool empty() { 
			//pthread_rwlock_rdlock(&rwlock);
			bool isEmpty = Queue.empty(); 
			//pthread_rwlock_unlock(&rwlock);
			return isEmpty;
		};
};

template <typename T>
using TMyQueue = typename MyQueue<T>::MyQueue;
//using TMyQueue = typename MyQueue<T>;

#include "MyQueue.cpp"

#endif /* MYQUEU_H_ */