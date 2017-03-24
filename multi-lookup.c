//
// Created by owatt on 3/15/2017.
//

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#include "util.h"
#include "queue.h"

#define MINARGS 3
#define USAGE "<inputFilePath> <outputFilePath>"
#define MAX_NAME_LIMIT 1025
#define SBUFSIZE 1025
#define INPUTFS "%1024s"
#define QUEUE_SIZE 25
#define MAX_RESOLVER_THREADS 2*sysconf( _SC_NPROCESSORS_ONLN ) // for extra credit

// declare globally because reasons
pthread_mutex_t queueLock;
pthread_mutex_t fileLock;
queue mainQueue;
int reading = 1;

void* InputThread(void* p) {
    FILE* outfp = p;

    char *payload;
    char hostname[MAX_NAME_LIMIT];

    int success = 0;
    int errorc = 0;

    while(fscanf(outfp, INPUTFS, hostname) > 0) {
        while(!success) {
            // acquire queue lock before performing operations on it
            errorc = pthread_mutex_lock(&queueLock);
            if (errorc) fprintf(stderr, "Queue mutex lock error %d\n", errorc);

            // check for queue being full before continuing
            if (queue_is_full(&mainQueue)) {
                errorc = pthread_mutex_unlock(&queueLock);
                if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);
                // sleep for random time from project requirements
                usleep((rand()%100)*1000);
            } else {
                // allocate space for the payload and push it into the queue
                payload = malloc(MAX_NAME_LIMIT);
                if (payload == NULL) fprintf(stderr, "Malloc error\n");

                payload = strncpy(payload, hostname, MAX_NAME_LIMIT);

                if (queue_push(&mainQueue, payload) == QUEUE_FAILURE) fprintf(stderr, "Queue push error");

                errorc = pthread_mutex_unlock(&queueLock);
                if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);

                // signal success
                success = 1;
            }
        }

        // reset success
        success = 0;
    }

    // signal that all inputs have been read
    reading = 0;
    // cleanup
    if (fclose(outfp)) fprintf(stderr, "Error closing file \n");
    return NULL;
}

void* OutputThread(void* p) {
    // pull data out from params
    FILE* output = p;

    char* hostname;
    char firstipstr[INET6_ADDRSTRLEN];

    int errorc = 0;

    while (!queue_is_empty(&mainQueue) || reading) {
        // acquire queue lock before any operations are preformed
        errorc = pthread_mutex_lock(&queueLock);
        if (errorc) fprintf(stderr, "Queue mutex lock error %d\n", errorc);

        // pop off next address from queue
        hostname = queue_pop(&mainQueue);

        // see if we have data and wait if not
        if (hostname == NULL) {
            errorc = pthread_mutex_unlock(&queueLock);
            if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);
            usleep((rand()%100)*1000);
        } else {
            // no longer need queue
            errorc = pthread_mutex_unlock(&queueLock);
            if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);

            /* finally do the one thing this program is supposed to do */
            if (dnslookup(hostname, firstipstr, sizeof(firstipstr)) == UTIL_FAILURE) {
                fprintf(stderr, "dns lookup failure: %s\n", hostname);
                strncpy(firstipstr, "", sizeof(firstipstr));
            }

            // lock file mutex for writing
            errorc = pthread_mutex_lock(&fileLock);
            if (errorc) fprintf(stderr, "File mutex lock error %d\n", errorc);

            // write to file
            errorc = fprintf(output, "%s,%s\n", hostname, firstipstr);
            if (errorc < 0) fprintf(stderr, "Output file write error\n");

            // unlock file mutex
            errorc = pthread_mutex_unlock(&fileLock);
            if (errorc) fprintf(stderr, "file mutex unlock error %d\n", errorc);

            // extra safe hostname free up for the next round
            free(hostname);
            hostname = NULL;
        }
    }
    return NULL;
}

int main(int argc, char* argv[]){

    int inFiles = argc-2;

    // build array of input files for no race condition in reader threads
    FILE* inputfp[inFiles];
    FILE* outputfp = NULL;

    pthread_t inThreads[inFiles];
    pthread_t outThreads[MAX_RESOLVER_THREADS];

    // local variable for iterating
    int i;

    /* Check Arguments */
    if(argc < MINARGS){
        fprintf(stderr, "Not enough arguments: %d\n", (argc - 1));
        fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
        return EXIT_FAILURE;
    }

    /* create queue and handle failure */
    if (queue_init(&mainQueue, QUEUE_SIZE) == QUEUE_FAILURE) {
        fprintf(stderr, "Error creating queue\n");
        return EXIT_FAILURE;
    }

    // for holding the creation errors and give the error number back
    int errorc;
    /* create and handle queue mutex */
    errorc = pthread_mutex_init(&queueLock, NULL);
    if (errorc) {
        fprintf(stderr, "Error creating the output file mutex\n");
        fprintf(stderr, "Error No: %d\n", errorc);
        return EXIT_FAILURE;
    }

    /* now again for file mutex */
    errorc = pthread_mutex_init(&fileLock, NULL);
    if (errorc) {
        fprintf(stderr, "Error creating the output file mutex\n");
        fprintf(stderr, "Error No: %d\n", errorc);
        return EXIT_FAILURE;
    }
    /* Open Output File */
    outputfp = fopen(argv[(argc-1)], "w");
    if(!outputfp){
        perror("Error Opening Output File");
        return EXIT_FAILURE;
    }

    /* open input files */
    for(i=1; i<(argc-1); i++){

        /* Open Input File */
        // i just love off by one errors
        inputfp[i-1] = fopen(argv[i], "r");
        if(!inputfp[i-1]){
            fprintf(stderr, "error opening file: %s", argv[i]);
            exit(EXIT_FAILURE);
        }
    }

    /* loop for input threads */
    for(i=0; i < inFiles; ++i) {

        errorc = pthread_create(&inThreads[i], NULL, InputThread, (void *)inputfp[i]);
        if (errorc) {
            fprintf(stderr, "couldn't create process thread: %d\n", errorc);
            exit(EXIT_FAILURE);
        }
    }

    /* now create output threads */
    for(i=0; i < MAX_RESOLVER_THREADS; ++i) {

        errorc = pthread_create(&outThreads[i], NULL, OutputThread, (void *)outputfp);
        if (errorc) {
            fprintf(stderr, "couldn't create process thread: %d\n", errorc);
            exit(EXIT_FAILURE);
        }
    }

    // join input threads
    for(i=0; i < inFiles; ++i) pthread_join(inThreads[i], NULL);

    // join output threads
    for(i=0; i < MAX_RESOLVER_THREADS; ++i) pthread_join(outThreads[i], NULL);

    /* Close Output File */
    if (fclose(outputfp)) fprintf(stderr, "Error closing output file\n");

    // cleanup
    queue_cleanup(&mainQueue);
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destroy(&fileLock);

    return EXIT_SUCCESS;
}
