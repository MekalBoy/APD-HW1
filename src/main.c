#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_BUFFER 512 // How big can a line be anyway?

struct args {
    int thread_id;
    pthread_barrier_t* mapstop; // Barrier that everyone syncs to
    int nr_files; // Number of files a mapper will handle
    int nr_bytes; // Total size of the files the mapper has
    struct fileinfo* files; // The files a mapper will process
};

struct fileinfo {
    char fileName[MAX_BUFFER];
    int id;
    int size;
    FILE* file;
};

struct wordList {
    pthread_mutex_t listMutex;
    int totalWords; // how many words are in the list
    int capacity; // how many words can there be in the list
    struct wordInfo** list;
};

struct wordInfo {
    char word[MAX_BUFFER];
    int timesFound; // in how many files is the word
    int filesFound[MAX_BUFFER];
};

void printArgs(struct args myargs) {
    printf("thread_id %d; nr_files %d\n", myargs.thread_id, myargs.nr_files);
    if (myargs.nr_files > 0) {
        for (int i = 0; i < myargs.nr_files; i++) {
            printf("File %d: id %d; filename %s; size %d\n", i, myargs.files[i].id, myargs.files[i].fileName, myargs.files[i].size);
        }
    }
}

unsigned long fsize(char *file)
{
    FILE *f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long) ftell(f);
    fclose(f);

    return len;
}

void processString(char *input, char *output) {
    int c = 0;
    for (int i = 0; input[i] != '\0'; i++) {
        if (input[i] == '.' || input[i] == ',' || input[i] == '?' || input[i] == '!') {
            // Skip dots and commas
            continue;
        }

        if (isalpha(input[i])) {
            output[c++] = tolower(input[i]);
        } else {
            output[c++] = input[i];
        }
    }

    output[c] = '\0';
}

struct wordInfo* findWord(struct wordList *list, char *word) {
    for (int i = 0; i < list->totalWords; i++) {
        if (strcmp(word, list->list[i]->word) == 0) {
            // printf("FOund %s before!\n", word);
            return list->list[i];
        }
    }

    return NULL;
}

void *mapper(void *arg) {
    struct args myargs = *(struct args *)arg;

    printf("Mapper %d started.\n", myargs.thread_id);

    printf("Mapper %d has %d files.\n", myargs.thread_id, myargs.nr_files);
    // printArgs(myargs);
    struct wordList localList;
    localList.capacity = MAX_BUFFER;
    localList.totalWords = 0;
    localList.list = malloc(localList.capacity * sizeof(struct wordInfo));

    char wordBuffer[MAX_BUFFER];
    char goodBuffer[MAX_BUFFER];
    if (myargs.nr_files > 0) { 
        for (int i = 0; i < myargs.nr_files; i++) {
            myargs.files[i].file = fopen(myargs.files[i].fileName, "r");

            // It's not going to read more than the usual size probably
            // char *hehe = malloc(myargs.files[i].size + MAX_BUFFER);
            // hehe[0] = '\0';
            // sprintf(hehe, "%d - ", myargs.thread_id);

            while (fscanf(myargs.files[i].file, "%s\n", wordBuffer) == 1) {
                processString(wordBuffer, goodBuffer);

                struct wordInfo *wordInList = findWord(&localList, goodBuffer);

                // If we haven't found the word before, we add it to the list
                if (wordInList == NULL) {
                    struct wordInfo newWord;
                    wordInList = &newWord;

                    strcpy(wordInList->word, goodBuffer);
                    wordInList->timesFound = 0;

                    if (localList.capacity == localList.totalWords - 2) {
                        localList.list = realloc(localList.list, localList.capacity * 2 * sizeof(struct wordInfo));
                        localList.capacity *= 2;
                    }

                    localList.list[localList.totalWords] = wordInList;
                    localList.totalWords++;
                }

                // Check to see if we found it before IN THIS FILE
                int foundInFile = 0;
                for (int j = 0; j < wordInList->timesFound && (!foundInFile); j++) {
                    if (wordInList->filesFound[j] == myargs.files[i].id) {
                        foundInFile = 1;
                    }
                }

                if (!foundInFile) {
                    // if (myargs.thread_id == 0)
                    //     printf("Found new word! %s in %d\n", goodBuffer, myargs.files[i].id);
                    wordInList->filesFound[wordInList->timesFound] = myargs.files[i].id;
                    wordInList->timesFound++;
                }
            }
            // printf("%s\n", hehe);

            // free(hehe);

            fclose(myargs.files[i].file);
        }
    }

    // Process everything locally, then write them into the masterList

    // take mutex
    // write to masterList
    // release mutex
    // if (myargs.thread_id == 0) {
    //     for (int i = 0; i < localList.totalWords; i++) {
    //         printf("[%s] in ", localList.list[i].word);
    //         for (int j = 0; j < localList.list[i]->timesFound; j++) {
    //             printf("%d ", localList.list[i].filesFound[j]);
    //         }
    //         printf("\n");
    //     }
    // }

    free(localList.list);

    pthread_barrier_wait(myargs.mapstop);
    return 0;
}

void *reducer(void *arg) {
    struct args myargs = *(struct args *)arg;

    // Reducers wait until all mappers have finished.
    pthread_barrier_wait(myargs.mapstop);

    printf("Reducer %d started.\n", myargs.thread_id);

    // Check vector of 26 mutexes+isTaken
    // take mutex
    // if isTaken == 0, isTaken = 1 and release mutex and start writing into the a,b,c etc files
    // - take file_mutex, write into file, release file_mutex
    // if isTaken == 1, release mutex
    // if past z, finish
    return 0;
}

int compareSizeDesc(const void *a, const void *b) {
    const struct fileinfo A = *(struct fileinfo *)a;
    const struct fileinfo B = *(struct fileinfo *)b;
    return (B.size - A.size);
}

void greedyPartition(struct fileinfo *files, int fileCount, int N, struct fileinfo **subsets, int *subsetSums, int *subsetCounts) {
    for (int i = 0; i < N; i++) {
        subsetSums[i] = 0;
        subsetCounts[i] = 0;
    }

    // Descending order
    qsort(files, fileCount, sizeof(struct fileinfo), compareSizeDesc);

    for (int i = 0; i < fileCount; i++) {
        int minSubset = 0;
        for (int j = 1; j < N; j++) {
            if (subsetSums[j] < subsetSums[minSubset]) {
                minSubset = j;
            }
        }

        subsets[minSubset][subsetCounts[minSubset]] = files[i];
        subsetCounts[minSubset]++;
        subsetSums[minSubset] += files[i].size;
    }
}

int main(int argc, char **argv)
{
    // Debug variable - mostly enables a lot of printfs
    int debug = 0;

    // Validate arguments

    if (argc < 4) {
        printf("Correct usage:\n./tema1 [numar_mapperi] [numar_reduceri] [fisier_intrare]\n");
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
        arguments[i].nr_bytes = 0;
        arguments[i].files = NULL;
    }

    struct fileinfo files[nr_files];
    int totalBytes = 0;

    int r;
    char lineBuffer[MAX_BUFFER];
    for (int i = 0; i < nr_files; i++) {
        r = fscanf(input_file, "%s\n", lineBuffer);

        if (r == EOF) {
            printf("Tried to read another line, but there are no more lines.\n");
            exit(1);
        }

        totalBytes += fsize(lineBuffer);
        // if (debug) {
        //     printf("File %s has size %lu.\n", lineBuffer, fsize(lineBuffer));
        // }

        struct fileinfo newFile;
        strcpy(newFile.fileName, lineBuffer);
        newFile.id = i;
        newFile.size = fsize(newFile.fileName);

        files[i] = newFile;
    }

    fclose(input_file);

    // Balance files for mapping

    struct fileinfo **subsets = (struct fileinfo **)malloc(nr_mappers * sizeof(struct fileinfo *));
    for (int i = 0; i < nr_mappers; i++) {
        subsets[i] = (struct fileinfo *)malloc(nr_files * sizeof(struct fileinfo)); // Maximum files per subset
    }

    int subsetSums[nr_mappers];
    int subsetCounts[nr_mappers];
    greedyPartition(files, nr_files, nr_mappers, subsets, subsetSums, subsetCounts);

    // Assign files in arguments

    for (int i = 0; i < nr_mappers; i++) {
        arguments[i].nr_files = subsetCounts[i];
        arguments[i].nr_bytes = subsetSums[i];
        arguments[i].files = subsets[i];

        // Realloc down to size that is actually in use
        arguments[i].files = realloc(arguments[i].files, arguments[i].nr_files * sizeof(struct fileinfo));
    }

    // if (debug) {
    //     for (int i = 0; i < nr_mappers; i++) {
    //         printf("Subset %d (Total %d):\n", i + 1, subsetSums[i]);
    //         for (int j = 0; j < subsetCounts[i]; j++) {
    //             printf("- File: %s, id: %d, size: %d\n", subsets[i][j].fileName, subsets[i][j].id, subsets[i][j].size);
    //         }
    //         printf("\n");
    //     }
    // }

    // Initialize threads

    // Note for self: last thread will be (NUM_THREADS - 1)
	for (int i = 0; i < NUM_THREADS; i++) {
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
    for (int i = 0; i < NUM_THREADS; i++) {
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

    for (int i = 0; i < NUM_THREADS; i++) {
        if (i < nr_mappers) {
            // Mappper arg freeing
            free(arguments[i].files);
        } else {
            // Reducer arg freeing
        }
    }

    pthread_barrier_destroy(&mapstop);

    // Initial pointer not needed anymore
    free(subsets);

    return 0;
}