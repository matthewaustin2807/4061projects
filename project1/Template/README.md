This program is essentially a basic map-reduce program which creates multiple processes on the same machine through fork-wait-exec sequences. To compile this program, simply type in `make` in the directory where the Makefile exists. Once everything is compiled, run the mapreduce program by typing the following: `./mapreduce #mappers #reducers filename` in the command line. However, currently this program is not yet complete since it is still created for the intermediate submission. Therefore currently, only the **mapreduce.c** is completed. This **mapreduce.c** file will spawn mappers and reducers according to the number supplied by the user. We know that it is currently working as we know each child process manages to call the mapper and reducer executables judging by the fact that print statements from mapper and reducer gets outputted to the command line.

Team Member Names and x500:
- Matthew Chandra (chand703)
- 

Contribution by each member:
- Both of us did further research on how fork-wait-exec system calls work as it was still unclear for us. After gaining knowledge on the syscalls, we separated the task to the following: Matthew did the part where mappers are spawned, while Arfan did the part where reducers are spawned.
