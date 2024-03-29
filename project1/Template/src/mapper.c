#include "mapper.h"

// combined value list corresponding to a word <1,1,1,1....>
valueList *createNewValueListNode(char *value){
	valueList *newNode = (valueList *)malloc (sizeof(valueList));
	strcpy(newNode -> value, value);
	newNode -> next = NULL;
	return newNode;
}

// insert new count to value list
valueList *insertNewValueToList(valueList *root, char *count){
	valueList *tempNode = root;
	if(root == NULL)
		return createNewValueListNode(count);
	while(tempNode -> next != NULL)
		tempNode = tempNode -> next;
	tempNode -> next = createNewValueListNode(count);
	return root;
}

// free value list
void freeValueList(valueList *root) {
	if(root == NULL) return;

	valueList *tempNode = root -> next;;
	while (tempNode != NULL){
		free(root);
		root = tempNode;
		tempNode = tempNode -> next;
	}
}

// create <word, value list>
intermediateDS *createNewInterDSNode(char *word, char *count){
	intermediateDS *newNode = (intermediateDS *)malloc (sizeof(intermediateDS));
	strcpy(newNode -> key, word);
	newNode -> value = NULL;
	newNode -> value = insertNewValueToList(newNode -> value, count);
	newNode -> next = NULL;
	return newNode;
}

// insert or update a <word, value> to intermediate DS
intermediateDS *insertPairToInterDS(intermediateDS *root, char *word, char *count){
	intermediateDS *tempNode = root;
	if(root == NULL)
		return createNewInterDSNode(word, count);
	while(tempNode -> next != NULL) {
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value = insertNewValueToList(tempNode -> value, count);
			return root;
		}
		tempNode = tempNode -> next;
		
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value = insertNewValueToList(tempNode -> value, count);
	} else {
		tempNode -> next = createNewInterDSNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeInterDS(intermediateDS *root) {
	if(root == NULL) return;

	intermediateDS *tempNode = root -> next;;
	while (tempNode != NULL){
		freeValueList(root -> value);
		free(root);
		root = tempNode;
		tempNode = tempNode -> next;
	}
}

//------------------------------------------------------------------
//------------------WORK STARTS HERE--------------------------------
//------------------------------------------------------------------

//STATIC Intermediate Data Structure
static intermediateDS* createdIDS;

// emit the <key, value> into intermediate DS 
void emit(char *key, char *value) {

	//the function will already check if a key exists.
	createdIDS = insertPairToInterDS(createdIDS, key, value);

}

// map function
void map(char *chunkData){
	// you can use getWord to retrieve words from the 
	// chunkData one by one. Example usage in utils.h
	int i = 0;
	char *buffer;
	while ((buffer = getWord(chunkData, &i)) != NULL){ //get next word in the file.
		//Add word to intermediate data structure (intermediateDS)
		emit(buffer, "1"); 

	}
}

// write intermediate data to separate word.txt files
// Each file will have only one line : word 1 1 1 1 1 ...
void writeIntermediateDS() {
	intermediateDS *tempIDSnode;
	tempIDSnode = createdIDS;
	//traverse through IDS and create new text files with the word and its counts (as raw 1's)
	while(tempIDSnode != NULL){ 
		char *newKey = tempIDSnode -> key; 

		//Open a new file for this word.
		FILE *mapFile; //Create new txt file to write the word and its instances.
		char filename[128];
		sprintf(filename, "%s/%s.txt", mapOutDir, newKey);
		mapFile = fopen(filename, "w"); //create new file in a folder output/MapOut and inside another folder labeled by Map_<mapperID> (defined mapper.h, so accessible)
		if (mapFile == NULL){
			printf("Failed to open file\n");
			return;
		}

		//Insert the word (key) and its count (value) into file.
		fprintf(mapFile, "%s", newKey); //Write word into file.
		valueList* tempValNode = tempIDSnode -> value; //get IDSnode's valueList.
		while (tempValNode != NULL){ //Write down all "valueList" nodes (count).
			fprintf(mapFile, " %s", tempValNode -> value); 
			tempValNode = tempValNode -> next;
		}
		 
		//Go to next word.
		tempIDSnode = tempIDSnode -> next;
		fclose(mapFile);
	}

	//DS iterated through successfully. Now free.
	freeInterDS(createdIDS);
}

//------------------------------------------------------------------
//------------------WORK ENDS HERE----------------------------------
//------------------------------------------------------------------

int main(int argc, char *argv[]) {
	
	if (argc < 2) {
		printf("Less number of arguments.\n");
		printf("./mapper mapperID\n");
		exit(0);
	}
	// ###### DO NOT REMOVE ######
	mapperID = strtol(argv[1], NULL, 10);

    //Print statement for mapper, comment this for final submission
    // printf("Mapper id : %d \n",mapperID);

	// ###### DO NOT REMOVE ######
	// create folder specifically for this mapper in output/MapOut
	// mapOutDir has the path to the folder where the outputs of 
	// this mapper should be stored
	mapOutDir = createMapDir(mapperID);

	// ###### DO NOT REMOVE ######
	while(1) {
		// create an array of chunkSize=1024B and intialize all 
		// elements with '\0'
		char chunkData[chunkSize + 1]; // +1 for '\0'
		memset(chunkData, '\0', chunkSize + 1);

		char *retChunk = getChunkData(mapperID);
		if(retChunk == NULL) {
			break;
		}

		strcpy(chunkData, retChunk);
		free(retChunk);

		map(chunkData);
	}

	// ###### DO NOT REMOVE ######
	writeIntermediateDS();

	return 0;
}
