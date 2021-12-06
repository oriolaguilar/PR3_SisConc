/* ---------------------------------------------------------------
Práctica 3.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */

#include "MapReduce.h"
#include "Types.h"

#include <dirent.h>
#include <string.h>
#include <mutex>
#include <pthread.h>

using namespace std;
#define MAXREDUCERS 5

int waiting_list[MAXREDUCERS];
pthread_mutex_t Mutex = PTHREAD_MUTEX_INITIALIZER; 
pthread_cond_t Buffer[MAXREDUCERS];

// Constructor MapReduce: directorio/fichero entrada, directorio salida, función Map, función reduce y número de reducers a utilizar.
MapReduce::MapReduce(char * input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers)
{
	MapFunction=mapf;
	ReduceFunction=reducef;
	InputPath = input;
	OutputPath = output;

	for(int x=0;x<nreducers;x++)
	{
		char filename[256];

		sprintf(filename, "%s/result.r%d", OutputPath, x+1);
		AddReduce(new TReduce(ReduceFunction, filename));
	}

	//CONTROL ERROOOOOOORS
	for (int i = 0; i < MAXREDUCERS; i++){
		Buffer[i] = PTHREAD_COND_INITIALIZER;
		pthread_mutex_init(&lock[i], NULL);
		pthread_cond_init(&Buffer[i], NULL);
	}
	pthread_mutex_init(&Mutex, NULL);
}

// Genera y lee diferentes splits: 1 split por fichero.
// Versión secuencial: asume que un único Map va a procesar todos los splits.
TError 
MapReduce::Split(char *input, int init, int end)
{
	PtrMap map = new TMap(MapFunction);
	AddMap(map);

	map->ReadFileTuples(input, init, end);

	return(COk);


}

// Ejecuta cada uno de los Maps.
TError 
MapReduce::Map(int m)
{
	if (debug) printf ("DEBUG::Running Map %d\n", (int)m+1);
	PtrMap top = GetMap();
	if (top->Run()!=COk)
		error("MapReduce::Map Run error.\n");
	
	pthread_mutex_lock (&Mutex);
	for (int i = 0; i < Reducers.size(); i++){
		mutexQueue("PUSH", i, top);
		pthread_cond_signal(&Buffer[i]);
	}
	printf("Map: %d\n", m);
	
	pthread_mutex_unlock (&Mutex);
	return(COk);
}

// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
// función de partición, para distribuir las claves entre los posibles reducers.
// Utiliza un multimap para realizar la ordenación/unión.
TError 
MapReduce::Suffle(int r, int totalMaps)
{
	TMapOuputIterator it2;
    
	for(int m = 0; m < totalMaps; m++) 
	{
		pthread_mutex_lock (&Mutex);
		while (map_shuffle[r].empty()){
			pthread_cond_wait(&Buffer[r], &Mutex);
		}
		pthread_mutex_unlock (&Mutex);
		printf("Es recolleix un map --- %d\n", r);
		multimap<string, int> output = mutexQueue("POP", r, NULL)->getOutput();
		printf("Es descarta el pop --- %d\n", r);

		// Process all mapper outputs
		for (TMapOuputIterator it1=output.begin(); it1!=output.end(); it1=it2)
		{
			//printf("Iteració n %d map shuffle\n", it1);
			TMapOutputKey key = (*it1).first;
			pair<TMapOuputIterator, TMapOuputIterator> keyRange = output.equal_range(key);

			// Calcular a que reducer le corresponde está clave:
			int possible_r = std::hash<TMapOutputKey>{}(key)%Reducers.size();

			if (possible_r == r){
				if (debug) printf ("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);
				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers[r]->AddInputKeys(keyRange.first, keyRange.second);
			}
			// Eliminar todas las entradas correspondientes a esta clave.
	        //for (it2 = keyRange.first;  it2!=keyRange.second;  ++it2)
	        //   output.erase(it2);
		output.erase(keyRange.first,keyRange.second);
		it2=keyRange.second;
		}
		printf("Arriba aqui?\n");

	}
	
	return(COk);
}

// Ejecuta cada uno de los Reducers.
TError 
MapReduce::Reduce(int m)
{
	//printf("PETA AQUI? m = %d\n", m);
	if (Reducers[m]->Run()!=COk)
		error("MapReduce::Reduce Run error.\n");
	//printf("NOOOOO m = %d\n", m);
	return (COk);
}

TError
MapReduce::cancelSyncro()
{
	for (int i = 0; i < MAXREDUCERS; i++){
		pthread_mutex_destroy(&lock[i]);
		pthread_cond_destroy(&Buffer[i]);
	}
	pthread_mutex_init(&Mutex, NULL);
	return (COk);

}

