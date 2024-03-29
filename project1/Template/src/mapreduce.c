#include "mapreduce.h"
#include <unistd.h>

pid_t r_wait(int *stat_loc) { 
	int retval;
	while (((retval = wait(stat_loc)) == -1) && (errno == EINTR)) ;
	return retval;
}

int main(int argc, char *argv[]) {
	
	if(argc < 4) {
		printf("Less number of arguments.\n");
		printf("./mapreduce #mappers #reducers inputFile\n");
		exit(0);
	}

	// ###### DO NOT REMOVE ######
	int nMappers 	= strtol(argv[1], NULL, 10);
	int nReducers 	= strtol(argv[2], NULL, 10);
	char *inputFile = argv[3];

	// ###### DO NOT REMOVE ######
	bookeepingCode();

	//handles if file doesnt exist of can't be opened
	int fileStatus = open(inputFile, O_RDONLY);
	if (fileStatus == -1){
		printf("File doesn't exist\n");
		exit(0);
	}

	// ###### DO NOT REMOVE ######
	pid_t pid = fork();
	if(pid == 0){
		//send chunks of data to the mappers in RR fashion
		sendChunkData(inputFile, nMappers);
		exit(0);
	}
	sleep(1);

	//handle if nMappers < nReducers
	if (nMappers < nReducers){
		printf("Number of mappers should be greater than or equal to the number of reducers\n");
		exit(0);
	}
	// To do
	// spawn mappers processes and run 'mapper' executable using exec
	pid_t *pidArrayMappers = malloc(sizeof(pid_t) * nMappers); //will store the pid of each child created so that it can be waited by the parent.
	int status;
	for (int i = 0; i < nMappers; i++){
		char mapID[5];
		sprintf(mapID, "%d", i+1);
		pid_t childpid = fork();
		//throw error if fork fails
		if (childpid == -1){
			perror("Failed to fork\n");
			return 1;
		}
		//child
		else if (childpid == 0){
			execlp("./mapper", "./mapper", mapID,  NULL);
			printf("Error executing\n");
			exit(0);
		}
		//parent
		else{
			pidArrayMappers[i] = childpid;
		}
	}
	// To do
	// wait for all children to complete execution
	for (int i = 0; i < nMappers; i++){
		waitpid(pidArrayMappers[i], &status, 0);
	}


	// ###### DO NOT REMOVE ######
    // shuffle sends the word.txt files generated by mapper 
    // to reducer based on a hash function
	pid = fork();
	if(pid == 0){
		shuffle(nMappers, nReducers);
		exit(0);
	}
	sleep(1);


	// To do
	// spawn reducer processes and run 'reducer' executable using exec
	pid_t *pidArrayReducers = malloc(sizeof(pid_t) * nReducers); //store the pid of the children so it can be waited by the parent.
	for (int i = 0; i < nReducers; i++){
		char mapID[5];
		sprintf(mapID, "%d", i+1);
		pid_t childpid = fork();
		//throw error if fork fails
		if (childpid == -1){
			perror("Fork creation failed\n");
			return 1;
		}
		//child 
		else if (childpid == 0){
			execlp("./reducer", "./reducer", mapID,  NULL);
			exit(0);
		}
		//parent
		else {
			pidArrayReducers[i] = childpid;
		}
	}
	// To do
	// wait for all children to complete execution
	for(int i = 0; i < nReducers; i++){
		waitpid(pidArrayReducers[i], &status, 0);
	}

	//free all of the arrays allocated to store the children pids.
	free(pidArrayMappers);
	free(pidArrayReducers);
	return 0;
}
