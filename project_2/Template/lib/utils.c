#include "utils.h"

#define BUFSIZE 1024

int key = 4061;

char *getChunkData(int mapperID) {
   //So what happens is that early in the program's execution, sendChunkData is called. In our scenario, sendChunkData acts as the "MASTER" phase.
   //
   //It then creates a brand new message queue.
   //The message queue is made up of a bunch of what I'd call "mini-queues", each identified by an "mtype" which corresponds to a mapperID.
   //(The mapperID is basically a unique identification based on the number of mappers the user wants to use.)
   //The "mini-queues" of each mtype contain chunks of data assigned to that mapperID proceeded by a special <END> message.
   //The number of chunks depends on the size of the data.
   //
   //So let's say we have an input file with 12,288 characters, and the user wants 4 mappers to calculate this.
   //So each mapper will be assigned 3072 chars.
   //Since a chunk can only be 1024 large, the mapper will further split their assigned parts further into 3 separate chunks 
   //So now the entire queue looks kinda like this: 
   //	message queue = 
   //	{
   //		mtype=1 -> "chunk - chunk - chunk - <END>"
   //		mtype=2 -> "chunk - chunk - chunk - <END>"
   //		mtype=3 -> "chunk - chunk - chunk - <END>"
   //		mtype=4 -> "chunk - chunk - chunk - <END>"
   //	}
   //(this is not a perfect representation. In cases when the data is not evenly split, some chunks can have smaller than 1024 sizes. Also when words can overlap.)
   //
   // Once sendChunkData finishes making the message queue, it idles and waits...
   //
   //Later in the program's execution, a number of "mapper" processes will be created, each with a unique mapperID. This indicates the "MAP" phase.
   //Expectedly, these mappers correspond to a "mini-queue", which both share an identical mapperID and mtype!
   //Each of these mapper processes will continuously call getChunkData() over and over with the argument being the mapper's ID (see line 205 of mapper.c).
   //As getChunkData() is called over and over by this mapper, the chunks of the mapper's "mini-queue" will eventually deplete.
   //This is indicated when getChunkData() reads an <END> instead of a chunk with this mapperID as its argument!.
   //
   //At this point of an <END>, getChunkData() will return to the mapper a NULL (telling the mapper that it's now empty),
   //and then getChunkData() writes into the "mini-queue" an <ACK> message (the mtype being 0xffffffff, which we reserve for communication).
   //sendChunkData() then reads this <ACK> message, which tells sendChunkData() that one of the "mini-queues" is finished being used.
   //
   //When sendChunkData() gets as many <ACK> messages as nMappers, it then closes the entire message queue and ends.
   
    int nReadByte = 0; //counter for # of bytes read (i.e. ensure all bytes of a message are read; no more, no less).
    struct msgBuffer buf; //Create a buffer to hold received data from message queue.
    int msqid;
  
  //TODO open message queue
   msqid = msgget(key, 0666); //We open the message queue to READ stuff from sendChunkData's message (it must already exist!)
   
   if(msqid == -1) {//check if opened?
       perror("no available queue for now"); //DO SOMETHING ELSE?
       return NULL;
   } else {

       //TODO receive chunk from the master
        nReadByte = msgrcv(msqid, &buf, sizeof(struct msgBuffer), mapperID, 0); //From the same queue used in send, take a message with the type (identification) "MapperID".
        if(nReadByte == -1) //failed to retreive message (error)
        {
            return NULL;
        } 
        
        //TODO check for END message and send ACK to master and return NULL. 
        //Otherwise return pointer to the chunk data. 
        //
        if (0 == strcmp(buf.msgText, "!!!!!")){ //Check if an END message is received (End = "!!!!!")
        	buf.msgType = 0xffffffff; //Send acknowledgement that an END message through a message of this special type.
        	strcpy(buf.msgText, "?????\0");//(Ack = "?????").
        	msgsnd(key, &buf, sizeof(struct msgBuffer), 0); //Send it back to master (i.e. sendChunkData)
        	return NULL;//Returns NULL to indicate to Mapper that this queue is empty.
        } else {
        	char* retChar = malloc(MSGSIZE); //prepare return value.
        	strcpy(retChar, buf.msgText);
        	return retChar; //send chunk for the mapper to store in the IntermediateDS
        	//(retChar will be deallocated by mapper.c)
        }

  }

}

void sendChunkData(char *inputFile, int nMappers) {
	struct msgBuffer msgbuf;
	char curWord[100];
	char buf[BUFSIZE];
	int msqid;

	//NEW: Current MapperID.
	int curMapper = 1;

	memset(curWord, '\0', 100);
	memset(buf, '\0', BUFSIZE);

	FILE* fd = fopen(inputFile, "r");
	// int fd = open(inputFile, O_RDONLY);
  //TODO open the message queue
	msqid = msgget(key, IPC_CREAT|0666);
	if (msqid == -1){
		perror("Fail to open or create the queue");
		return;
	}

  //TODO Construct chunks of at most 1024 bytes each and send each chunk to a mapper in a round
  // robin fashion. Read one word at a time(End of word could be  \n, space or ROF). If a chunk 
  // is less than 1024 bytes, concatennate the word to the buffer.   
  //Note: Round robin is like passing cards in a card game. for nMappers = 5 -> 1,2,3,4,5,1,2,3,4,5, etc.
	//WHERE DID HE FIND GETNEXTWORD??
	//Needs to be rewritten for getWord.
	//Pseudocode:
	//	Get a 1024 chunk from the file (use matt's code for this)
	//	Send it to getWord to filter invalid words.
	//	Send it to the msgqueue
	//How does getWord work?
	// One argument passes a chunk, i is the counter that returns the length of the chunk
	// But why does getWord ask for a chunk? Wouldn't it make sense to send it a file?
	int i = 0; //current position (i because getWord's arg is i)
	fseek(fd, 0L, SEEK_END);
	int fileSize = ftell(fd); //length of file.
	fseek(fd, 0L, SEEK_SET);
	char* fileContents = malloc(sizeof(char) * fileSize);//entire text content of file.

	char c;
	while ( EOF != (c = fgetc( fd )) && ++i < fileSize ){ //load entire content of file.
            fileContents[i] = c;
	}

    fclose (fd); //close file.

	while (i < fileSize){//iterate through all the file's contents
		char* wordBuffer;
		wordBuffer = getWord(fileContents, &i); //get next word from file
		if (strlen(wordBuffer) + strlen(msgbuf.msgText) > chunkSize){ //send chunk to mem queue
			msgbuf.msgType = curMapper++; //roundrobin fashion.
			if(curMapper > nMappers){//next loop sends for first mapper again.
				curMapper = 1;
			}
			if(msgsnd(msqid, &msgbuf, sizeof(struct msgBuffer), 0) == -1) //failed to send message chunk (error)
			    {
				perror("msgop: msgsnd failed");
				break;
			    }
			memset(msgbuf.msgText, '\0', MSGSIZE);//clear message text for next chnk.
		}
		free(wordBuffer);//clear buffer.
	}

	free(fileContents);//file fully iterated.
	
	
	// while (getNextWord(fd, &curWord) != 0){// Something wrong here... getNextWord doesn't exist!
	// 	if (strlen(buf) + strlen(curWord) > 1024){ //chunk filled up.
	// 		msgbuf.msgType = curMapper++; //The mapper to assign this chunk to (round-robin fashion, so increment to the next mapper for the next loop)
	// 		strcpy(msgbuf.msgText, buf);
	// 		msgbuf.msgText[strlen(buf)] = '\0';//top it off with null terminator.
	// 		if(msgsnd(msqid, &msgbuf, sizeof(struct msgBuffer), 0) == -1) //failed to send message chunk (error)
	// 		    {
	// 			perror("msgop: msgsnd failed");
	// 			break;
	// 		    }
	// 		if(curMapper > nMappers){//Go back to the first mapper when all mappers passed a chunk in this iteration.
	// 			curMapper = 1;
	// 		}
	// 	} else { //chunk not full. Add to chunk!
	// 		strcat(buf, curWord); 
	// 		strcat(buf, " ");
	// 	}
	// }

  //TODO inputFile read complete, send END message to mappers
	for (int i = 1; i <= nMappers; i++){
		struct msgBuffer msgbuf;
		msgbuf.msgType = i;//add an END to the ends of each of the mapper queues.
		strcpy(msgbuf.msgText, "!!!!!");//(End = "!!!!!")
		msgbuf.msgText[strlen("!!!!!")] = '\0';//top the end message with a \0.
		if (msgsnd(msqid, &msgbuf, sizeof(struct msgBuffer), 0) == -1) //failed to send message chunk (error)
            {
                perror("msgop: msgsnd failed");
                break;
            }
	}

  //TODO wait to receive ACK from all mappers for END notification
	//Simple behavior: From a special type (make in our case, maximum val of int), count how many ENDs were received.
	int mapperCount = 0;
	while (mapperCount < nMappers){
		int nReadByte;
		nReadByte = msgrcv(msqid, &msgbuf, sizeof(struct msgBuffer), 0xffffffff, 0);//the 0xffffffff key is used for communication
	 	if (nReadByte == -1){ //When does this happen?? Error?
	 		break;
	 	}
		if (strcmp(msgbuf.msgText, "?????")){ //Ack message received (Ack = "?????")
			mapperCount += 1;
		}

	}
	//Matt's code: 
	// int i = 0;
	// while (i < nMappers){
	// 	int nReadByte;
	// 	nReadByte = msgrcv(msqid, &msgbuf, sizeof(struct msgBuffer), 1, 0);

	// 	// msgbuf.mtext[nReadByte] = '\0'; //not necessary, since we already top all sent messages with \0? (TA Told me)
	// 	if (strcmp(msgbuf.mtext, "ACK") == 0){
	// 		i++;
	// 	}
	// }

	msgctl(msqid, IPC_RMID, NULL); //close message queue.
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
