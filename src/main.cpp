#include <ctype.h>
#include <fstream>
#include <pthread.h>
#include <queue>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <iostream>
#include <unordered_map>

using namespace std;

#define MAX_BUFFER 512 // How big can a line be anyway?

struct fileinfo {
    char fileName[MAX_BUFFER];
    int id;
    int size;
};

struct wordList {
    pthread_mutex_t listMutex; // not used locally - only on masterList
    std::unordered_map<string, vector<int>> list;
};

struct writingQueue {
    pthread_mutex_t queueMutex;
    std::queue<char> queue;
};

struct args {
    int thread_id;
    pthread_barrier_t* mapstop; // Barrier that everyone syncs to
    int nr_files; // Number of files a mapper will handle
    int nr_bytes; // Total size of the files the mapper has (used for debugging)
    struct fileinfo* files; // The files a mapper will process
    struct wordList* masterList; // The list every mapper will write to and reducers will read from
    struct writingQueue* writeQueue; // The list from where reducers get their writing assignments
};

// Debug function
void printArgs(struct args myargs) {
    printf("thread_id %d; nr_files %d\n", myargs.thread_id, myargs.nr_files);
    if (myargs.nr_files > 0) {
        for (int i = 0; i < myargs.nr_files; i++) {
            printf("File %d: id %d; filename %s; size %d\n", i, myargs.files[i].id, myargs.files[i].fileName, myargs.files[i].size);
        }
    }
}

// Returns the size of a file
unsigned long fsize(char *file)
{
    FILE *f = fopen(file, "r");
    fseek(f, 0, SEEK_END);
    unsigned long len = (unsigned long) ftell(f);
    fclose(f);

    return len;
}

// Sanitize input and write into output
void processString(string& input, string& output) {
    output.clear(); // Clear output first
    for (char c : input) {
        if (isalpha(c)) {
            output += tolower(c);
        }
    }
}

bool compareWordlists(const std::pair<string, vector<int>>& a, const std::pair<string, vector<int>>& b) {
    if (a.second.size() != b.second.size()) {
        return a.second.size() > b.second.size();
    }

    // If sizes are equal, compare by word
    return a.first < b.first;
}

void *mapper(void *arg) {
    struct args myargs = *(struct args *)arg;

    printf("Mapper %d started.\n", myargs.thread_id);

    printf("Mapper %d has %d files.\n", myargs.thread_id, myargs.nr_files);

    // Partial list that's written at the end, when it's filled out
    struct wordList localList;

    if (myargs.nr_files > 0) { 
        for (int i = 0; i < myargs.nr_files; i++) {
            ifstream file;
            file.open(myargs.files[i].fileName);

            string word;
            string goodWord;
            while ( file >> word ) {
                processString(word, goodWord);
                
                vector<int> &wordInfo = localList.list[goodWord];

                // If it doesn't exist or it exists but without the current file id
                if (wordInfo.empty() || std::find(wordInfo.begin(), wordInfo.end(), myargs.files[i].id) == wordInfo.end()) {
                    wordInfo.push_back(myargs.files[i].id);
                }
            }

            file.close();
        }
    }

    // Processed everything locally, now to write them into the masterList

    pthread_mutex_lock(&myargs.masterList->listMutex);

    for (auto& wordInfo : localList.list) {
        const std::string& word = wordInfo.first;
        std::vector<int>& localFileIds = wordInfo.second;

        // Check if the word already exists in the master list
        auto it = myargs.masterList->list.find(word);
        if (it == myargs.masterList->list.end()) {
            // Word does not exist, add it with file id
            myargs.masterList->list[word] = localFileIds;
        } else {
            // Word exists, update its vector of file IDs
            std::vector<int>& masterFileIds = it->second;
            
            // Iterate through the local file IDs and add any that are missing
            for (int localFileId : localFileIds) {
                if (std::find(masterFileIds.begin(), masterFileIds.end(), localFileId) == masterFileIds.end()) {
                    // Not found in this file before, insert it
                    masterFileIds.push_back(localFileId);
                }
            }
        }
    }

    pthread_mutex_unlock(&myargs.masterList->listMutex);

    pthread_barrier_wait(myargs.mapstop);

    return 0;
}

void *reducer(void *arg) {
    struct args myargs = *(struct args *)arg;

    // Reducers wait until all mappers have finished.
    pthread_barrier_wait(myargs.mapstop);

    printf("Reducer %d started.\n", myargs.thread_id);

    // Storing to sort as I wish
    vector<std::pair<string, vector<int>>> sortedWords;

    for (auto& [word, files] : myargs.masterList->list) {
        sortedWords.push_back({word, files});
    }

    std::sort(sortedWords.begin(), sortedWords.end(), &compareWordlists);

    while (1) {
        // Take from the queue

        pthread_mutex_lock(&myargs.writeQueue->queueMutex);

        if (myargs.writeQueue->queue.empty()) {
            pthread_mutex_unlock(&myargs.writeQueue->queueMutex);
            break;
        }

        char currentChar = myargs.writeQueue->queue.front();
        myargs.writeQueue->queue.pop();

        pthread_mutex_unlock(&myargs.writeQueue->queueMutex);

        // Make due with current character

        ofstream file;
        
        char fileName[10];
        sprintf(fileName, "%c.txt", currentChar);
        file.open(fileName);

        for (auto& [word, files] : sortedWords) {
            if (word[0] == currentChar) {
                file << word << ":[";
                
                std::sort(files.begin(), files.end());

                // g++ screams at me if I don't use size_t
                for (size_t i = 0; i < files.size(); i++) {
                    file << files[i];
                    if (i < files.size() - 1) { // if not last element
                        file << " ";
                    }
                }

                file << "]\n";
            }
        }

        file.close();
    }

    return 0;
}

int compareSizeDesc(const void *a, const void *b) {
    const struct fileinfo A = *(struct fileinfo *)a;
    const struct fileinfo B = *(struct fileinfo *)b;
    return (B.size - A.size);
}

// https://en.wikipedia.org/wiki/Greedy_number_partitioning -> https://en.wikipedia.org/wiki/Longest-processing-time-first_scheduling
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

    struct wordList masterList;
    pthread_mutex_init(&masterList.listMutex, NULL);

    struct writingQueue masterQueue;
    pthread_mutex_init(&masterQueue.queueMutex, NULL);
    for (char c = 'a'; c <= 'z'; c++) {
        masterQueue.queue.push(c);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        arguments[i].nr_files = 0;
        arguments[i].nr_bytes = 0;
        arguments[i].files = NULL;
        arguments[i].masterList = &masterList;
        arguments[i].writeQueue = &masterQueue;
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

        struct fileinfo newFile;
        strcpy(newFile.fileName, lineBuffer);
        newFile.id = i + 1;
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
    }

    if (debug) {
        for (int i = 0; i < nr_mappers; i++) {
            printf("Subset %d (Total %d):\n", i + 1, subsetSums[i]);
            for (int j = 0; j < subsetCounts[i]; j++) {
                printf("- File: %s, id: %d, size: %d\n", subsets[i][j].fileName, subsets[i][j].id, subsets[i][j].size);
            }
            printf("\n");
        }
    }

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

    // Initial pointer not needed anymore
    free(subsets);

    for (int i = 0; i < NUM_THREADS; i++) {
        if (i < nr_mappers) {
            // Mappper arg freeing
            free(arguments[i].files);
        } else {
            // Reducer arg freeing
        }
    }

    pthread_barrier_destroy(&mapstop);
    pthread_mutex_destroy(&masterList.listMutex);
    pthread_mutex_destroy(&masterQueue.queueMutex);

    return 0;
}