/* ---------------------------------------------------------------
Práctica 3.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */

#ifndef MYQUEU_H_
#define MYQUEU_H_

#include <queue>
#include <pthread.h>

using namespace std;
	
template<class T>
class MyQueue
{
	pthread_rwlock_t 	rwlock = PTHREAD_RWLOCK_INITIALIZER;
	std::queue<T> 		Queue;
	
	public:
		MyQueue();
		
		inline void push (T &val) { 
			pthread_rwlock_wrlock(&rwlock);
			Queue.push(val); 
			pthread_rwlock_unlock(&rwlock);
		};
		inline void pop() {
			if (!Queue.empty()){
				pthread_rwlock_wrlock(&rwlock);
				Queue.pop();
				pthread_rwlock_unlock(&rwlock);
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
