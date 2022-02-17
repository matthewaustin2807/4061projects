#include "reducer.h"

// create a key value node
finalKeyValueDS *createFinalKeyValueNode(char *word, int count){
	finalKeyValueDS *newNode = (finalKeyValueDS *)malloc (sizeof(finalKeyValueDS));
	strcpy(newNode -> key, word);
	newNode -> value = count;
	newNode -> next = NULL;
	return newNode;
}

// insert or update an key value
finalKeyValueDS *insertNewKeyValue(finalKeyValueDS *root, char *word, int count){
	finalKeyValueDS *tempNode = root;
	if(root == NULL)
		return createFinalKeyValueNode(word, count);
	while(tempNode -> next != NULL){
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value += count;
			return root;
		}
		tempNode = tempNode -> next;
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value += count;
	} else{
		tempNode -> next = createFinalKeyValueNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeFinalDS(finalKeyValueDS *root) {
	if(root == NULL) return;

	finalKeyValueDS *tempNode = root -> next;;
	while (tempNode != NULL){
		free(root);
		root = tempNode;
		tempNode = tempNode -> next;
	}
}

//------------------------------------------------------------------
//------------------WORK STARTS HERE--------------------------------
//------------------------------------------------------------------

//STATIC final intermediateDS
static finalKeyValueDS* createdFDS;

// reduce function
// Processes one word at a time.
void reduce(char *key) {
	char buffer[MAXKEYSZ];
	char word[MAXKEYSZ];
	int counter = 0;

	//open the map file. "key" is the raw path to the file. 
	FILE* toReduce;
	toReduce = fopen(key, "r"); //open map file.
	if (toReduce == NULL){
		printf("File can't be opened\n");
		return;
	}

	//Get word from file.
	fscanf(toReduce, "%s", buffer); 
	strcpy(word, buffer); 

	//Count instances of word.
	while ( fscanf(toReduce, "%s", buffer) != EOF ) counter++;  //counts how many "1s" (instances) there are in the map.

	//Insert word/counter into the data structure.

	createdFDS = insertNewKeyValue(createdFDS, word, counter);

	//Close file.
	fclose(toReduce);
	
}

// write the contents of the final intermediate structure
// to output/ReduceOut/Reduce_reducerID.txt
void writeFinalDS(int reducerID){
	char toPut[MAXKEYSZ];

	finalKeyValueDS* nodeFDS = createdFDS;

	//Created file to insert reduced data.	
	char filename[128];
	sprintf(filename, "output/ReduceOut/Reduce_%d.txt", reducerID);
	FILE *reducedFile;
	reducedFile = fopen(filename, "w");
	if (reducedFile == NULL){
		printf("File can't be opened\n");
		return;
	}
	
	//Writing lines one by one.
	while(nodeFDS != NULL){
		strcpy(toPut, nodeFDS->key);
		strcat(toPut, " ");
		char val[128];
		sprintf(val, "%d", nodeFDS->value);
		strcat(toPut, val);
		strcat(toPut, "\n");
		fputs(toPut, reducedFile);
		nodeFDS = nodeFDS->next;
	}
	
	//DS iterated through successfully. Now free.
	freeFinalDS(createdFDS);
	fclose(reducedFile);

}

//------------------------------------------------------------------
//------------------WORK ENDS HERE----------------------------------
//------------------------------------------------------------------

int main(int argc, char *argv[]) {

	if(argc < 2){
		printf("Less number of arguments.\n");
		printf("./reducer reducerID");
	}

	// ###### DO NOT REMOVE ######
	// initialize 
	int reducerID = strtol(argv[1], NULL, 10);

    //Print statement for reducer, comment this for final submission
    printf("Reducer id : %d \n",reducerID);

	// ###### DO NOT REMOVE ######
	// master will continuously send the word.txt files
	// alloted to the reducer
	char key[MAXKEYSZ];
	while(getInterData(key, reducerID))
		reduce(key);

	// You may write this logic. You can somehow store the
	// <key, value> count and write to Reduce_reducerID.txt file
	// So you may delete this function and add your logic
	writeFinalDS(reducerID);

	return 0;
}
