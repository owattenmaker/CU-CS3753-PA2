//
// Created by owatt on 3/15/2017.
//

#include "multi-lookup.h"
#include "queue.h"

#define MINARGS 3
#define USAGE "<inputFilePath> <outputFilePath>"
#define MAX_NAME_LIMIT 1025
#define SBUFSIZE 1025
#define INPUTFS "%1024s"

#define QUEUE_SIZE 25
#define MAX_RESOLVER_THREADS 10
#define CU_CS3753_PA2_MULTI_LOOKUP_H

int main(int argc, char* argv[]){

    int inFiles = argc-2;

    // build array of input files for no race condition in reader threads
    FILE* inputfp[inFiles];
    FILE* outputfp = NULL;

    pthread_mutex_t queueLock;
    pthread_mutex_t fileLock;

    pthread_t inThreads[inFiles];
    pthread_t outThreads[MAX_RESOLVER_THREADS];

    queue mainQueue;

    inFunP inputParams[inFiles];
    outFunP outParams[MAX_RESOLVER_THREADS];

    /* Local Vars */
    char hostname[SBUFSIZE];
    char errorstr[SBUFSIZE];
    char firstipstr[INET6_ADDRSTRLEN];
    int i, t;
    int alive = 1;

    /* Check Arguments */
    if(argc < MINARGS){
        fprintf(stderr, "Not enough arguments: %d\n", (argc - 1));
        fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
        return EXIT_FAILURE;
    }

    /* create queue and handle failure */
    if (queue_init(&mainQueue, QUEUE_SIZE) != QUEUE_SUCCESS) {
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

    /* Loop Through Input Files */
    for(i=1; i<(argc-1); i++){

        /* Open Input File */
        // i just love off by one errors
        inputfp[i-1] = fopen(argv[i], "r");
        if(!inputfp[i-1]){
            sprintf(errorstr, "Error Opening Input File: %s", argv[i]);
            perror(errorstr);
            break;
        }

        /* Read File and Process*/
        while(fscanf(inputfp, INPUTFS, hostname) > 0){

            /* Lookup hostname and get IP string */
            if(dnslookup(hostname, firstipstr, sizeof(firstipstr))
               == UTIL_FAILURE){
                fprintf(stderr, "dnslookup error: %s\n", hostname);
                strncpy(firstipstr, "", sizeof(firstipstr));
            }

            /* Write to Output File */
            fprintf(outputfp, "%s,%s\n", hostname, firstipstr);
        }

        /* Close Input File */
        fclose(inputfp);
    }

    /* Close Output File */
    fclose(outputfp);

    return EXIT_SUCCESS;
}
