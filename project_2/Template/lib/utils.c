#include "utils.h"

//Message buffer struct:
//long mtype
//char mtext[1024]

//key= 4061

// "SIGNALING" MESSAGES
//ACK = SOMETHING?
//END = SOMETHING?


char *getChunkData(int mapperID) {
   //So what happens is that early in the program's execution, sendChunkData is called and creates a message queue which holds the data of ALL the chunks there 
   // (after which, sendChunkData idles and waits for the queue to be	 fully used before closing it)
   //When getChunkData is called, elements from this queue (i.e. the chunks) is retreived and sent to the whatever called it.
   //If some excessively large mapperID is getChunkData's argument (maybe #ofMappers + 1?), getChunkData will see an "END" message in the message queue!   
   //This function is used in mapper.c.
   //Mapper.c will call getChunkData from 1,2,...#ofmapperIDs.
   //Eventually, mapper.c will call getChunkData(#ofmapperIDs + 1) (more mappers than those that exist), and when getChunkData tries to find a message in the message queue with this ID, it should find an END MESSAGE.
   //When getChunkData sees this END MESSAGE, it will send an ACKNOLEDGEMENT MESSAGE to sendChunkData before returning NULL.
   //When sendChunkData sees this ACKNOWLEDGE MESSAGE in the queue, it is the signal to that this mapper is done getting all its data...
   //And when all mappers are done getting the data, the message queue will be closed.
   //, and getChunkData replies with an acknowledgement of that message. 
   
    int nReadByte = 0; //counter for # of bytes read (i.e. ensure all bytes of a message are read; no more, no less).
    struct my_msgbuf buf; //Create a buffer to hold received data from message queue.
  
  //TODO open message queue
   msqid = msgget(KEY, 0666); //We open the message queue to READ stuff from sendChunkData's message (it must already exist!)
   
   if(msqid == -1) {//check if opened?
       perror("no available queue for now"); //DO SOMETHING ELSE?
       return -1;
   } else {

       //TODO receive chunk from the master
        nReadByte = msgrcv(msqid, &buf, sizeof(struct my_msgbuf), mapperID, 0); //From the same queue used in send, take a message with the type (identification) "MapperID".
        if(nReadByte == -1) //failed. No message.
        {
            break;
        } 
        
        //TODO check for END message and send ACK to master and return NULL. 
        //Otherwise return pointer to the chunk data. 
        //
        if (0 == strcmp(buf.mtext, "^&*()") && buf.mtype == <INSERT ENDTYPE>){ //Check if an END message is received (You define this matt!)
        	//Create an acknowledgement message (using buf, since its contents are not useful).
        	//Acknowledg defined as: Type 6024752, Text = "!@#$%" (the text are all invalid chars for a word).
        	buf.mtype = 6024752;
        	buf.mtext = "!@#$%\0";                	
        	msgsnd(key, buf, sizeof(my_msgbuf), 0); //Send it back to master (i.e. sendChunkData)
        	return NULL;
        } else {
        	return buf.mtext;
        }

  }
  


}

void sendChunkData(char *inputFile, int nMappers) {
  //TODO open the message queue


  //TODO Construct chunks of at most 1024 bytes each and send each chunk to a mapper in a round
  // robin fashion. Read one word at a time(End of word could be  \n, space or ROF). If a chunk 
  // is less than 1024 bytes, concatennate the word to the buffer.   
  


  //TODO inputFile read complete, send END message to mappers


  //TODO wait to receive ACK from all mappers for END notification

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
