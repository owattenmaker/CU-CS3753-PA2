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

struct inFunction {
    FILE* file_name;
    pthread_mutex_t* qLock;
    queue* q;
} inFunP;

struct outFunction {
    FILE* file_name;
    pthread_mutex_t* qLock;
    pthread_mutex_t* oLock;
    queue *q;
    int* writing;
} outFunP;


void* InputThread(void* p) {
    char host[MAX_NAME_LIMIT];

    inFunP *params = p;

    FILE *name = params->file_name;
    pthread_mutex_t *queueLock = params->qLock;
    queue *mainqueue = params->q;

    char *payload;

    int success, errorc = 0;

    while(fscanf(name, INPUTFS, host) > 0) {
        while(!success) {
            errorc = pthread_mutex_lock(queueLock);
            if (errorc) fprintf(stderr, "Queue mutex lock error %d\n", errorc);

            // check for queue being full before continuing
            if (queue_is_full(mainqueue)) {
                errorc = pthread_mutex_unlock(queueLock);
                if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);
                // sleep for random time from project requirements
                usleep((rand()%100)*1000);
            } else {
                // allocate space for the payload and push it into the queue
                payload = malloc(MAX_NAME_LIMIT);
                if (payload == NULL) fprintf(stderr, "Malloc error\n Warning results non-deterministic\n");

                payload = strncpy(payload, host, MAX_NAME_LIMIT);

                if (queue_push(mainqueue, payload) == QUEUE_FAILURE) fprintf(stderr, "Queue push error");

                errorc = pthread_mutex_unlock(queueLock);
                if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);

                // signal success
                success = 1;

            }
        }

        // reset success
        success = 0;
    }

    // cleanup
    if (fclose(name)) fprintf(stderr, "Error closing file \n");
    return NULL;
}

void* OutputThread(void* p) {
    // pull data out from params
    outFunP* params = p;

    FILE* output = params->file_name;
    pthread_mutex_t* queueLock = params->qLock;
    pthread_mutex_t* fileLock = params->fLock;
    queue* mainqueue = params->q;
    int* writing = params->writing;

    char* hostname;
    char ipaddress[INET6_ADDRSTRLEN];

    int errorc = 0;

    while (*writing || !queue_is_empty(mainqueue)) {
        // aquire lock
        errorc = pthread_mutex_lock(queueLock);
        if (errorc) fprintf(stderr, "Queue mutex lock error %d\n", errorc);

        // pop off next address from queue
        hostname = queue_pop(mainqueue);

        // see if we have data and wait if not
        if (hostname == NULL) {
            errorc = pthread_mutex_unlock(queueLock);
            if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);
            usleep((rand()%100)*1000);
        } else {
            // no longer need queue
            errorc = pthread_mutex_unlock(queueLock);
            if (errorc) fprintf(stderr, "Queue mutex unlock error %d\n", errorc);

            /* finally do the one thing this program is supposed to do */
            if (dnslookup(hostname, ipaddress, sizeof(ipaddress)) == UTIL_FAILURE) {
                fprintf(stderr, "dns lookup failure: %s\n", hostname);
                strncpy(ipaddress, "", sizeof(ipaddress));
            }

            // lock file mutex for writing
            errorc = pthread_mutex_unlock(fileLock);
            if (errorc) fprintf(stderr, "File mutex lock error %d\n", errorc);

            // write to file
            errorc = fprintf(output, "%s,%s\n", hostname, ipaddress);
            if (errorc < 0) fprintf(stderr, "Output file write error\n");

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
    int i;
    int writing = 1;

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
            fprintf(stderr, "error opening file: %s", argv[i]);
            break;
        }
    }

    /* loop for input threads */
    for(i=0; i < inFiles; ++i) {
        FILE* current = inputfp[i];
        inputParams[i].qLock = &queueLock;
        inputParams[i].file_name = current;
        inputParams[i].q = &mainQueue;

        errorc = pthread_create(&inThreads[i], NULL, InputThread, &inputParams[i]);
        if (errorc) {
            fprintf(stderr, "couldn't create process thread: %d\n", errorc);
            exit(EXIT_FAILURE);
        }
    }

    /* now create output threads */
    for(i=0; i < MAX_RESOLVER_THREADS; ++i) {
        outParams[i].qLock = &queueLock;
        outParams[i].file_name = current;
        outParams[i].q = &mainQueue;
        outParams[i].outL = &fileLock;
        outParams[i].writing = &writing;

        errorc = pthread_create(&inThreads[i], NULL, InputThread, &inputParams[i]);
        if (errorc) {
            fprintf(stderr, "couldn't create process thread: %d\n", errorc);
            exit(EXIT_FAILURE);
        }
    }

    fprintf("Joining input threads\n");
    for(i=0; i < inFiles; ++i) pthread_join(inThreads[i], NULL);

    // set writing bit to 0 to signal
    writing = 0;
    fprintf("Joining output threads\n");
    for(i=0; i < MAX_RESOLVER_THREADS; ++i) pthread_join(outThreads[i], NULL);

    /* Close Output File */
    if (fclose(outputfp)) fprintf(stderr, "Error closing output file");

    // cleanup
    queue_cleanup(&mainQueue);
    pthread_mutex_destroy(&queueLock);
    pthread_mutex_destory(&fileLock);

    return EXIT_SUCCESS;
}
