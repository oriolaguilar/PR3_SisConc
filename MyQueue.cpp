/* ---------------------------------------------------------------
Práctica 3.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */
//#include "MyQueue.h"

//#include <queue>

#include <pthread.h>

template <class T>
MyQueue<T>::MyQueue()
{
//	Queue = new std::queue<T>(); 
	pthread_rwlock_init(&rwlock, NULL);
}

//template class MyQueue<int>;