#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_BUFFER 64 // How big can a line be anyway?

struct args {
    int thread_id;
    pthread_barrier_t* mapstop; // Barrier that everyone syncs to
    int nr_files; // Number of files a mapper will handle
    struct fileinfo* files; // The files a mapper will process
};

struct fileinfo {
    char fileName[MAX_BUFFER];
    int id;
    FILE* file;
};

void printArgs(struct args myargs) {
    printf("thread_id %d; nr_files %d\n", myargs.thread_id, myargs.nr_files);
    if (myargs.nr_files > 0) {
        for (int i = 0; i < myargs.nr_files; i++) {
            printf("file %d: id %d; filename %s;\n", i, myargs.files[i].id, myargs.files[i].fileName);
        }
    }
}

unsigned long fsize(char* file)
{
    FILE *f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long) ftell(f);
    fclose(f);

    return len;
}

void *mapper(void *arg) {
    struct args myargs = *(struct args *)arg;

    printf("%d Mapper started\n", myargs.thread_id);

    printf("%d Mapper has %d files.\n", myargs.thread_id, myargs.nr_files);
    printArgs(myargs);

    char wordBuffer[MAX_BUFFER];
    if (myargs.nr_files > 0) { 
        for (int i = 0; i < myargs.nr_files; i++) {
            myargs.files[i].file = fopen(myargs.files[i].fileName, "r");

            while (fscanf(myargs.files[i].file, "%s\n", wordBuffer) == 1) {
                printf("%s ", wordBuffer);
            }
            printf("\n");

            fclose(myargs.files[i].file);
        }
    }

    pthread_barrier_wait(myargs.mapstop);
    return 0;
}

void *reducer(void *arg) {
    struct args myargs = *(struct args *)arg;

    // Reducers wait until all mappers have finished.
    pthread_barrier_wait(myargs.mapstop);

    printf("%d Reducer started\n", myargs.thread_id);
    return 0;
}

int main(int argc, char **argv)
{
    // Debug variable - mostly enables a lot of printfs
    int debug = 1;

    // Validate arguments

    if (argc < 4) {
        printf("Correct usage:\n./tema1 [numar_mapperi] [numar_reduceri] [fisier_intrare]");
        exit(1);
    }

    int nr_mappers = atoi(argv[1]);
    int nr_reducers = atoi(argv[2]);
    FILE* input_file = NULL;
    input_file = fopen(argv[3], "r");

    if (nr_mappers < 1 || nr_reducers < 1) {
        printf("Incorrect number of mappers/reducers.\n");
        exit(1);
    }

    if (input_file == NULL) {
        printf("Could not open entry file.\n");
        exit(1);
    }

    // Process input file

    int nr_files = 0;
    fscanf(input_file, "%d\n", &nr_files);

    if (nr_files < 0) {
        printf("No files?\n");
        exit(1);
    }

    if (debug) {
        printf("Inputs: %d %d %s\n", nr_mappers, nr_reducers, argv[3]);
    }

    int NUM_THREADS = nr_mappers + nr_reducers;

    pthread_barrier_t mapstop;
    pthread_barrier_init(&mapstop, NULL, NUM_THREADS);

    // Compose arguments

    pthread_t threads[NUM_THREADS];
	struct args arguments[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        arguments[i].nr_files = 0;
        arguments[i].files = NULL;

        // fix this pls
        arguments[i].files = malloc(sizeof(struct fileinfo) * nr_files);
    }


    int i, r;
    char lineBuffer[MAX_BUFFER];
    for (i = 0; i < nr_files; i++) {
        r = fscanf(input_file, "%s\n", lineBuffer);

        if (r == EOF) {
            printf("Tried to read another line, but there are no more lines.\n");
            exit(1);
        }

        if (debug) {
            printf("File %s has size %lu.\n", lineBuffer, fsize(lineBuffer));
        }

        struct fileinfo newFile;
        strcpy(newFile.fileName, lineBuffer);
        // newFile.file = fopen(lineBuffer, "r");
        newFile.id = i;

        // Even distribution, kinda bad -- should be done based on fsize
        int lucky_mapper = i % nr_mappers;
        arguments[lucky_mapper].files[arguments[lucky_mapper].nr_files] = newFile;
        arguments[lucky_mapper].nr_files++;
    }

    // arguments[0].files[0].id = 1;
    // strcpy(arguments[0].files[0].fileName, "hehe");
    // printArgs(arguments[0]);

    // Initialize threads

    // Note for self: last thread will be (NUM_THREADS - 1)
	for (i = 0; i < NUM_THREADS; i++) {
		arguments[i].thread_id = i;
        arguments[i].mapstop = &mapstop;

        if (i < nr_mappers) {
    		r = pthread_create(&threads[i], NULL, &mapper, &arguments[i]);
        } else {
            r = pthread_create(&threads[i], NULL, &reducer, &arguments[i]);
        }

		if (r) {
			printf("Thread creation failed for %d ", i);
            if (i < nr_mappers) {
                printf("(mapper)\n");
            } else {
                printf("(reducer)\n");
            }
			exit(-1);
		}
	}

    // Await threads

    void *status;
    for (i = 0; i < NUM_THREADS; i++) {
		r = pthread_join(threads[i], &status);

		if (r) {
			printf("Error on wait for thread %d ", i);
            if (i < nr_mappers) {
                printf("(mapper)\n");
            } else {
                printf("(reducer)\n");
            }
			exit(-1);
		}
	}

    // Wrap-up (free & close)

    pthread_barrier_destroy(&mapstop);
    fclose(input_file);

    return 0;
}