#include "utils.h"
#include <stdio.h>

#define BUFSIZE 1024

int key = 4061;
char *getChunkData(int mapperID) {
  //TODO open message queue
   


  //TODO receive chunk from the master
    


  //TODO check for END message and send ACK to master and return NULL. 
  //Otherwise return pointer to the chunk data. 
  //
}

//Return the next word as an output parameter.
//Return 1: it reads a word. Return 0: it reaches the end of the
//stream. 
int getNextWord(int fd, char* buffer){
   char word[100];
   memset(word, '\0', 100);
   int i = 0;
   while(read(fd, &word[i], 1) == 1 ){
    if(word[i] == ' '|| word[i] == '\n' || word[i] == '\t'){
        strcpy(buffer, word);
        return 1;
    }
    if(word[i] == 0x0){
      break;
    }

    i++;
   }
   strcpy(buffer, word);
   return 0;
}

void sendChunkData(char *inputFile, int nMappers) {
	struct msgBuffer msgbuf;
	char curWord[100];
	char buf[BUFSIZE];

	memset(curWord, '\0', 100);
	memset(buf, '\0', BUFSIZE);

	int fd = open(inputFile, O_RDONLY);
  //TODO open the message queue
	msqid = msgget(key, IPC_CREAT|0666);
	if (msqid == -1){
		perror("Fail to open or create the queue");
		return 1;
	}

  //TODO Construct chunks of at most 1024 bytes each and send each chunk to a mapper in a round
  // robin fashion. Read one word at a time(End of word could be  \n, space or ROF). If a chunk 
  // is less than 1024 bytes, concatennate the word to the buffer.   
	while (getNextWord(fd, curWord) != 0){
		if (strlen(buf) + strlen(curWord) > 1024){
			msgbuf.msgType = 1;
			strcpy(msgbuf.msgText, buf);
			msgbuf.msgText[strlen(buf)] = '\0';
			if(msgsnd(msqid, &msgbuf, sizeof(struct msgBuffer), 0) == -1)
            {
                perror("msgop: msgsnd failed");
                break;
            }
		} else {
			strcat(buf, curWord);
			strcat(buf, ' ');
		}
	}
  //TODO inputFile read complete, send END message to mappers
	for (int i = 0; i < nMappers; i++){
		struct msgBuffer msgbuf;
		msgbuf.msgType = 1;
		strcpy(msgbuf.msgText, "END");
		msgbuf.msgText[strlen("END")] = '\0';
		if (msgsnd(msqid, &msgbuf, sizeof(struct msgBuffer), 0) == -1)
            {
                perror("msgop: msgsnd failed");
                break;
            }
	}

  //TODO wait to receive ACK from all mappers for END notification
	int i = 0;
	while (i < nMappers){
		int nReadByte;
		nReadByte = msgrcv(msqid, &msgbuf, sizeof(struct msgBuffer), 1, 0);
		if (nReadByte == -1){
			break;
		}
		msgbuf.mtext[nReadByte] = '\0';
		if (strcmp(msgbuf.mtext, "ACK") == 0){
			i++;
		}
	}

	msgctl(msqid, IPC_RMID, NULL);
}

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    //TODO open message queue
      



    //TODO receive data from the master
     



    //TODO check for END message and send ACK to master and then return 0
    //Otherwise return 1


}

void shuffle(int nMappers, int nReducers) {

    //TODO open message queue
     


    //TODO traverse the directory of each Mapper and send the word filepath to the reducer
    //You should check dirent is . or .. or DS_Store,etc. If it is a regular
    //file, select the reducer using a hash function and send word filepath to
    //reducer 




    //TODO inputFile read complete, send END message to reducers

    


    
    //TODO  wait for ACK from the reducers for END notification
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
