/* ---------------------------------------------------------------
Práctica 1.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */

#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"

#include <functional>
#include <queue>
#include <string>
#include <pthread.h>

class MapReduce 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;


		
	MyQueue<PtrMap> Mappers;
	std::queue<PtrMap> map_shuffle[5];
	vector<PtrReduce> Reducers;
	pthread_mutex_t lock[5];

	public:
		MapReduce(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers);
		TError Run();

		TError Split(char *input, int init, int end);
		TError Map(int i);
		TError Suffle(int m, int totalSplits);
		TError Reduce(int m);
		TError cancelSyncro();

	private:		
		inline void AddMap(PtrMap map) { Mappers.push(map); };
		
		inline PtrMap GetMap(){
			PtrMap top_map = Mappers.front();
			Mappers.pop();
			return top_map;

		}

		inline PtrMap mutexQueue(string action, int i, PtrMap var){
			pthread_mutex_lock(&lock[i]);
			PtrMap top = NULL;
			if (action == "PUSH"){
				map_shuffle[i].push(var);
			}else if (action == "POP"){
				top = map_shuffle[i].front();
				map_shuffle[i].pop();
			}
			pthread_mutex_unlock(&lock[i]);
			return top;
		}
		
		inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;


#endif /* MAPREDUCE_H_ */
