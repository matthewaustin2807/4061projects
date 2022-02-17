- test machine: csel-kh1250-31
- date: 02/16/2022
- name: Matthew A. Chandra, Muhammad Arfan Maulana
- x500: chand703, maula006

This program is essentially a basic map-reduce program which creates multiple processes on the same machine through fork-wait-exec sequences. To compile this program, simply type in `make` in the directory where the Makefile exists. Once everything is compiled, run the mapreduce program by typing the following: `./mapreduce #mappers #reducers filename` in the command line. The codebase for this program contains three essential files: mapreduce.c, mapper.c, and reducer.c. These files are interconnected with one another in a way that mapreduce will create an exact number of mappers and reducers as specified by the user. These mappers will then map the contents of the input file passed in, creating multiple intermediate data structures that stores how many occurence of a word there is in the input file. Reducers will then take advantage of these data structures to actually count and output the total number of occurence of a word.

Team Member Names and x500:
- Matthew Chandra (chand703)
- Muhammad Arfan Maulana (maula006)

Contribution by each member:
- Both of us did further research on how fork-wait-exec system calls work as it was still unclear for us. After gaining knowledge on the syscalls, we separated the task to the following: Matthew did the part where mappers are spawned, while Arfan did the part where reducers are spawned.
- Arfan led the process of drawing up the main logic behind the program, while Matthew led the coding part of the project by doing syntax fixing and bug removals.
