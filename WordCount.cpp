/* ---------------------------------------------------------------
Pràctica 3.
Código fuente: WordCount.c
Grau Informàtica
49256840V Oriol Aguilar Larruy.
48254146P Guillem Guardiola Agustí.
--------------------------------------------------------------- */

#include "Types.h"
#include "MapReduce.h"

#include <sstream>
#include <stdlib.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>
#include <unistd.h>


using namespace std;

#define MAX_FILES 100
#define MAX_SIZE 8338608

pthread_barrier_t barrier; 


struct File_rPosition {
    char* path;
    int init_reading;
    int end_reading;
    bool last;
};

struct map_and_files {
	PtrMapReduce map;
	struct File_rPosition files;
	int i;
	int splits;
};

TError MapWordCount(PtrMap, TMapInputTuple tuple);
TError ReduceWordCount(PtrReduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end);

int files_param(char *input, struct File_rPosition files[]);
void cancelThreads(pthread_t *threads, int n);
long file_size(char *name);
void *splitMap(void *parameters);
void *shuffle(void *parameters);
void *reduce(void *parameters);

int main(int argc, char *argv[])
{
	char *output_dir = (char *)malloc(sizeof(char)*50);
	char *input_dir  = (char *)malloc(sizeof(char)*50);

	// Procesar argumentos.
	if (argc != 3)
		error("Error in arguments: WordCount <input dir> <ouput dir>.\n");

	input_dir = argv[1];
	output_dir = argv[2];

	int num_reducers = 2;
	PtrMapReduce mapr = new TMapReduce(input_dir, output_dir, MapWordCount, ReduceWordCount, num_reducers);

	if (mapr == NULL)
		error("Error new MapReduce.\n");

	struct File_rPosition files[MAX_FILES];
	int num_splits = files_param(input_dir, files);
	
	pthread_barrier_init(&barrier, NULL, num_reducers + 1);		

	pthread_t split_threads[num_splits];
	for (int i = 0; i < num_splits; i++){
		struct map_and_files *parameters = new struct map_and_files(); 
		parameters->files.init_reading = files[i].init_reading;
		parameters->files.end_reading = files[i].end_reading;
		parameters->files.path = strdup(files[i].path);
		parameters->map   = mapr;
		parameters->i	 = i;
		if (pthread_create(&split_threads[i], NULL, splitMap, parameters) != 0)
			perror("Error crear threads");
	}

	//sleep(25);
	pthread_t shuffle_threads[num_reducers];
	for (int n = 0; n < num_reducers; n++){
		struct map_and_files *parameters = new struct map_and_files();
		parameters->map   = mapr;
		parameters->i     = n;
		parameters->splits = num_splits;
		if (pthread_create(&shuffle_threads[n], NULL, shuffle, parameters) != 0)
			perror("Error crear threads");
	}
	printf("Ha passatt?\n");
	pthread_barrier_wait (&barrier);
	
	/*for (int i = 0; i < num_reducers; i++)
		pthread_join(shuffle_threads[i], NULL);*/

	printf("SIIII. Ha passat\n");

	pthread_t reduce_threads[num_reducers]; 
	for (int i = 0; i < num_reducers; i++){
		struct map_and_files *parameters = new struct map_and_files();
		parameters->map   = mapr;
		parameters->i     = i;
		if (pthread_create(&reduce_threads[i], NULL, reduce, parameters) != 0)
			perror("Error crear threads");
	}
	void *RetCode;
	for (int i = 0; i < num_reducers; i++ ){
		if (pthread_join(reduce_threads[i], &RetCode) !=0)
			perror("Join Threads");
		if (RetCode < 0)
			cancelThreads(reduce_threads, num_reducers);
		reduce_threads[i]=0;
	}

	mapr->cancelSyncro();
	pthread_barrier_destroy(&barrier);
	exit(0);
}

void cancelThreads(pthread_t* threads, int n){
	for (int i = 0; i < n; i++){
		if (threads[i] != 0) 
			if (pthread_cancel(threads[i])!=0) 
				perror("Cancel Threads");
	}
}

void *shuffle(void *parameters){
	struct map_and_files *file = (struct map_and_files *)parameters;
	file->map->Suffle(file->i, file->splits);
	pthread_barrier_wait(&barrier);

}

void *reduce(void *parameters){
	struct map_and_files *file = (struct map_and_files *)parameters;
	file->map->Reduce(file->i);
}

void *splitMap(void *parameters){
	struct map_and_files *file = (struct map_and_files *)parameters;
	file->map->Split(file->files.path, file->files.init_reading, file->files.end_reading);
	file->map->Map(file->i);
}

int files_param(char *input, struct File_rPosition files[])
{
	DIR *dir;
	unsigned char isFile = 0x8;
	struct dirent *entry;
	int i = 0;
	if ((dir = opendir(input)) != NULL)
	{
		while ((entry = readdir(dir)) != NULL)
		{
			if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && entry->d_type == isFile)
			{
				char file_path[257];
				sprintf (file_path, "%s/%s", input, entry->d_name);
				files[i].init_reading = 0;
				files[i].end_reading = MAX_SIZE;
				files[i].last = false;
				files[i].path = strdup(file_path);
				//strcpy(files[i].path, file_path);
				int byte_counter = MAX_SIZE;
				long int size = file_size(file_path);
				while (size > byte_counter)
				{
					i += 1;
					files[i].init_reading = byte_counter + 1;
					files[i].end_reading = byte_counter + MAX_SIZE;
					files[i].last = false;
					files[i].path = strdup(file_path);
					//strcpy(files[i].path, file_path);
					byte_counter += MAX_SIZE;
				}
				i += 1;
			}
		}
		closedir(dir);
	}
	else
	{
		files[i].init_reading = 0;
		files[i].end_reading = MAX_SIZE;
		files[i].path = strdup(input);
		//strcpy(files[i].path, input);
		files[i].last = false;
		long int size = file_size(input);
		int byte_counter = MAX_SIZE;
		while (size > byte_counter)
		{
			i += 1;
			files[i].init_reading = byte_counter + 1;
			files[i].end_reading = byte_counter + MAX_SIZE;
			files[i].last = false;
			files[i].path = strdup(input);
			//strcpy(files[i].path, input);
			byte_counter += byte_counter;
		}
		i += 1;
	}
	return i;
}

long file_size(char *name)
{
	FILE *fp = fopen(name, "rb"); // must be binary read to get bytes

	long size = -1;
	if (fp)
	{
		fseek(fp, 0, SEEK_END);
		size = ftell(fp) + 1;
		fclose(fp);
	}
	return size;
}

// Word Count Map.
TError MapWordCount(PtrMap map, TMapInputTuple tuple)
{
	string value = tuple.getValue();

	if (debug)
		printf("DEBUG::MapWordCount procesing tuple %ld->%s\n", tuple.getKey(), tuple.getValue().c_str());

	// Convertir todos los posibles separadores de palabras a espacios.
	for (int i = 0; i < value.length(); i++)
	{
		if (value[i] == ':' || value[i] == '.' || value[i] == ';' || value[i] == ',' ||
			value[i] == '"' || value[i] == '\'' || value[i] == '(' || value[i] == ')' ||
			value[i] == '[' || value[i] == ']' || value[i] == '?' || value[i] == '!' ||
			value[i] == '%' || value[i] == '<' || value[i] == '>' || value[i] == '-' ||
			value[i] == '_' || value[i] == '#' || value[i] == '*' || value[i] == '/')
			value[i] = ' ';
	}

	stringstream ss;
	string temp;
	ss.str(value);
	// Emit map result (word,'1').
	while (ss >> temp)
		map->EmitResult(temp, 1);

	return (COk);
}

// Word Count Reduce.
TError ReduceWordCount(PtrReduce reduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end)
{
	TReduceInputIterator it;
	int totalCount = 0;

	if (debug)
		printf("DEBUG::ReduceWordCount key %s ->", key.c_str());

	// Procesar todas los valores para esta clave.
	for (it = begin; it != end; it++)
	{
		if (debug)
			printf(" %d", it->second);
		totalCount += it->second;
	}

	if (debug)
		printf(".\n");

	reduce->EmitResult(key, totalCount);

	return (COk);
}
