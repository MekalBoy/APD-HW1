# APD-HW1

This is an implementation of the Map-Reduce paradigm, specifically intended to find the reversed index for a set of input files.

### The current solution is split into three main parts:
1) Main Thread - reads input and assigns mapper workloads
2) Mapper Thread - processes loads, writes partial results
3) Reducer Thread - sorts and organizes partial results, writes into output

### Main
The main thread simply reads and parses the inputs. Arguments are validated to make sure they're not invalid. Following that, the files are opened one by one, composing them (along with their sizes) into a set that will be split as the Mapper workloads.
Using a [greedy partioning algorithm](https://en.wikipedia.org/wiki/Greedy_number_partitioning) the main set is split into nearly-equal subsets, which are then assigned to the arguments which will be passed to the Mapper threads.
The main thread also takes care to initialize any other necessities, such as mutexes or barriers, before starting the worker threads and awaiting them. Ultimately, after they all finish, the main thread will handle cleanup by freeing any memory allocations as well as destroying the previously-created mutexes/barriers.
A single barrier is used for syncing the transition between Mappers and Reducers, initialized with M+R.
A single structure is also used for the arguments. Even though this is inadvisable (as it leads to useless arguments being passed to Mapper & Reducer), it helps with simplicity.

### Mapper
Upon receiving the workloads through their arguments, the mappers get to work. A structure similar to the masterList is created locally to hold _partial_ results. The mapper goes through each assigned file, through each word within, checks it against its local "database" (if the word is missing, it is added along with the current file_id; if the word is there but the current file_id is missing, it is appended to the vector) and eventually writes the acquired results to the masterList (with a mutex to make sure there's no accidental overwriting).
Lastly the mappers go on to wait at the barrier. Once all mappers arrive at the barrier, the barrier opens- allowing mappers to exit and reducers to start.

### Reducer
The reducers start by waiting at the barrier. This helps make sure they only start once the mappers have all finished writing their results to the masterList, filling it out.
The masterList is further processed by the reducers (independently even though it's the same result) to sort the word-vector pairs, first by the vector length, then lexicographically by the words (keys) themselves in case vector lengths are the same.
The reducers then go on to forever check (synchronously, via mutex) a queue for any contained "tickets". The queue's elements are set in Main, specifically 26 characters from the english alphabet. These "tickets" are used to assign reducers the current file output they'll have to handle. All words that begin with that character will be written in order (along with their id vectors) to the file. Once the reducer finishes his ticket, he goes on to wait and grab another one, repeating the process anew with another file.
Once the tickets run out, reducers exit, having finished their job.

### Misc
Initially the program was written in C. Once I realized that C++ is also allowed, and once I hit a slight roadblock with efficiency (caused, apparently, by an incorrect way of reading/storing the words), I switched to C++. The code is simpler with C++, as I can make use of hashmaps, vectors, std::find, std::sort and the ever-useful auto and iterators.
A mix of C and C++ may be noticed throughout the program, though I hope it's not too distracting.

For additional reading, please check details on the model [here](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) or check the task itself [here](https://curs.upb.ro/2024/pluginfile.php/225728/mod_resource/content/2/Tema1a.pdf).