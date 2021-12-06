#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>


void *hilo_actiu();
void *hilo_pasiu();
void print_log(char* log);

pthread_mutex_t Mutex;
pthread_cond_t Buffer, Buffer2;

int main() {
    pthread_t tid[2];

	pthread_mutex_init(&Mutex, NULL);
    pthread_cond_init(&Buffer, NULL);
    pthread_cond_init(&Buffer2, NULL);

    if (pthread_create(&tid[0], NULL, (void*)hilo_actiu, NULL) != 0) {
        perror("Hilo Productor"); exit(-1);
    }
		
    if (pthread_create(&tid[1], NULL, (void*)hilo_pasiu, NULL) != 0) {
		perror("Hilo Productor"); exit(-1);
    }

    pthread_join(tid[0], NULL);
	pthread_join(tid[1], NULL);
   	pthread_cond_destroy(&Buffer);

}

void *hilo_actiu() {

    while (1 == 1)
    {
        int ret = pthread_cond_signal(&Buffer);
        printf("%d ", ret);
        print_log("--->");
        sleep(1);
    }
    

}

void *hilo_pasiu() {
    pthread_mutex_lock (&Mutex);
    while (1 == 1)
    {
        pthread_cond_wait(&Buffer, &Mutex);
        sleep(1);
        print_log("|||||");

    }
    pthread_mutex_unlock (&Mutex);

    

}

void print_log(char* log){
    char buff[20];
    struct tm *sTm;

    time_t now = time (0);
    sTm = gmtime (&now);

    strftime (buff, sizeof(buff), "%H:%M:%S", sTm);
    printf ("%s %s\n", buff, log);
}




