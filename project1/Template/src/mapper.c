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

	//Is it this simple???
	//the function will already check if a key exists.
	createdIDS = insertPairToInterDS(createdIDS, key, value)

}

// map function
void map(char *chunkData){
	//Create new intermediate Data Structure (DS) to store the word and its count (valueList; managed automatically)
	//Will be initialized later in the while loop (insert new node!)
	//note: DO NOT USE CREATE! Insert will do it for you!
	// you can use getWord to retrieve words from the 
	// chunkData one by one. Example usage in utils.h
	int i = 0;
	char *buffer;
	while ((buffer = getWord(chunkData, &i)) != NULL){
		//Add value to intermediate data structure (intermediateDS)
		emit(buffer, 1);
		//That's it.
	}
	//Done. No need to do anything else! main will call 
	
}

// write intermediate data to separate word.txt files
// Each file will have only one line : word 1 1 1 1 1 ...
void writeIntermediateDS() {
	//Note:
	//Wait so... there already exists an intermediateDS somewhere? Main has access to it somehow.
	//I created "createdIDS" as the static DS anyway
	//Do a for each of sorts that iterates through all words in "createdIDS" to fork. 
	//Create new files for each.
	//Fork unnecessary?
	//Free the DS after it all!
	intermediateDS *tempIDSnode;
	tempIDSnode = createdIDS;
	while(tempIDSnode != NULL){ //traverse through IDS
		char *newKey = tempIDSnode -> key; 
		FILE *newFile; //Create new txt file to write the word and its instances.
		newFile = fopen("%s/%s.txt", mapOutDir, newKey); //create new file in a folder output/MapOut and inside another folder labeled by Map_<mapperID> (defined mapper.h, so accessible)
		fprintf(newFile, "%s", newKey); //Write word into file.
		valueList* tempValNode = tempIDSnode -> value; //get IDSnode's valueList.
		while (tempValNode != NULL){ //traverse through current IDS Node's valuelist.
			fprintf(newFile, " %s", tempValNode -> value); //Write down all "valueList" nodes.
			tempValNode = tempValNode -> next;
		}
		tempIDSnode = tempIDSnode -> next; //Go to next word.
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
    printf("Mapper id : %d \n",mapperID);

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
