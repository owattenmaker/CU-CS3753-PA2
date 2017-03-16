//
// Created by owatt on 3/15/2017.
//

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>

#include "queue.h"
#include "util.h"

/* all taken from lookup.c */
#define MINARGS 3
#define USAGE "<inputFilePath> <outputFilePath>"
#define MAX_NAME_LIMIT 1025
#define BUFSIZE 1024
#define INPUTFS "%1024s"

#define QUEUE_SIZE 25
#define MAX_RESOLVER_THREADS 10
#ifndef CU_CS3753_PA2_MULTI_LOOKUP_H
#define CU_CS3753_PA2_MULTI_LOOKUP_H

typedef struct inFunction {
    FILE* file_name;
    pthread_mutex_t* queueL;
    queue* q;
} inFunP;

typedef struct outFunction {
    FILE* file_name;
    pthread_mutex_t* queueL;
    pthread_mutex_t* outL;
    queue *q;
    int* alive;
} outFunP;

void *InputThread(void *p);
void *OutputThread(void *p);

#endif //CU_CS3753_PA2_MULTI_LOOKUP_H
