#include "server.h"

//Global Variables [Values Set in main()]
int queue_len           = INVALID;                              //Global integer to indicate the length of the queue
int cache_len           = INVALID;                              //Global integer to indicate the length or # of entries in the cache        [Extra Credit B]
int num_worker          = INVALID;                              //Global integer to indicate the number of worker threads
int num_dispatcher      = INVALID;                              //Global integer to indicate the number of dispatcher threads
uint dynamic_flag       = INVALID_FLAG;                         //Global flag to indicate if the dynamic poool is being used or not         [Extra Credit A]
uint cache_flag         = INVALID_FLAG;                         //Global flag to indicate if the cache is being used or not                 [Extra Credit B]
struct sigaction action;                                        //Global signal handling structure for gracefully terminating from SIGINT
FILE *logfile;                                                  //Global file pointer for writing to log file in worker


/* ************************ Globals From Solution **********************************/

//Global Values
int cache_evict_index   = 0;                                      //Global integer for tracking the next next cache entry to evict
int req_remove_index    = 0;                                      //Global integer for tracking the next queue entry to remove from
int req_insert_index    = 0;                                      //Global integer for tracking the index into the request queue
int curr_queue_len      = 0;                                      //Global integer for tracking the current size of the queue

//Global Thread Tracking
pthread_t dispatch_threads[MAX_THREADS];                          //Global array of dispatcher thread's
pthread_t worker_threads[MAX_THREADS];                            //Global array of worker thread's
int id_arr[MAX_THREADS];                                          //Global Array to hold thread ID's
pthread_t dynamic_pool_deamon;                                    //Global thread ID for dynamic pool thread

//Global Thread Synchronization Items
pthread_mutex_t req_queue_mutex   = PTHREAD_MUTEX_INITIALIZER;    //Request Queue lock for thread safe dispatcher
pthread_cond_t req_queue_notfull  = PTHREAD_COND_INITIALIZER;     //Request Queue conditional for signaling queue is open
pthread_cond_t req_queue_notempty = PTHREAD_COND_INITIALIZER;     //Request Queue conditional for signaling queue is not empty
pthread_mutex_t cache_mutex       = PTHREAD_MUTEX_INITIALIZER;    //Cache lock for thread safe cache access in worker
pthread_mutex_t log_mutex         = PTHREAD_MUTEX_INITIALIZER;    //Log lock for thread safe logging

//Global request queue
request_t requests[MAX_QUEUE_LEN];

//Global cache
cache_entry_t* cache;

/**********************************************************************************/


/*
  THE CODE STRUCTURE GIVEN BELOW IS JUST A SUGGESTION. FEEL FREE TO MODIFY AS NEEDED
*/

/* ************************ Signal Handler Code **********************************/
void gracefulTerminationHandler(int sig_caught) {
  /* TODO (D.I)
  *    Description:      Mask SIGINT signal, so the signal handler does not get interrupted (this is a best practice)
  *    Hint:             See Lab Code
  */
  #pragma region SOLUTION CODE --> Install Signal Hanlder for Graceful Termination
  if(signal(SIGINT, SIG_IGN) == SIG_ERR){
    perror("ERROR: Could not ignore interrupt signal");
    exit(-1);
  }
  #pragma endregion

  /* TODO (D.II)
  *    Description:      Print to stdout the number of pending requests in the request queue
  *    Hint:             How should you check the # of remaining requests? This should be a global... Set that number to num_remn_req before print
  */
  int num_remn_req = -1;

  #pragma region SOLUTION CODE --> Fetch number of remaining requests from global
  num_remn_req = curr_queue_len;
  #pragma endregion
  
  printf("\nGraceful Termination: There are [%d] requests left in the request queue\n", num_remn_req);

  /* TODO (D.III)
  *    Description:      Terminate Server by closing threads, need to close threads before we do other cleanup
  *    Hint:             How should you handle running threads? How will the main function exit once you take care of threads?
  *                      If you are using the dynamic pool thread, you should stop that thread [Extra Credit A]
  *                      pthread_cancel will be your friend here... look at the boottom of server.h for helpful functions to be able to cancel the threads
  */
  #pragma region SOLUTION CODE --> Terminate Threads
  for (int i = 0; i < num_dispatcher; i++) {
    if(pthread_cancel(dispatch_threads[i]) < 0){
      printf("ERROR: Could not kill dispatcher thread [%ld] @ index [%d]\n", dispatch_threads[i], i);
      perror("could not kill\n");
      exit(-1);
    }
    pthread_cond_broadcast(&req_queue_notempty);
  }
  printf("SUCCESS: All dispatch thread's killed\n");

  for (int i = 0; i < num_worker; i++) {
    if(pthread_cancel(worker_threads[i]) < 0){
      printf("ERROR: Could not kill worker thread [%ld] @ index [%d]\n", worker_threads[i], i);
      perror("could not kill\n");
      exit(-1);
    }
  }
  printf("SUCCESS: All worker thread's killed\n");

  if(dynamic_flag){
    if(pthread_cancel(dynamic_pool_deamon) < 0){
        printf("ERROR: Could not kill dynamic pool thread [%ld]\n", dynamic_pool_deamon);
        perror("could not kill\n");
        exit(-1);
    }
    printf("SUCCESS: Dynamic pool thread killed\n");
  }
  #pragma endregion


  /* TODO (D.IV)
  *    Description:      Close the log file
  */
  #pragma region SOLUTION CODE --> Close the Logfile
  if(fclose(logfile) != 0){
    perror("ERROR: Could not close logfile... terminating from here\n");
    exit(-1);
  }else{
    printf("SUCCESS: Closed logfile\n");
  }
  #pragma endregion

  /* TODO (D.V)
  *    Description:      Remove the cache by calling deleteCache if using cache [Extra Credit B]
  */
  #pragma region SOLUTION CODE --> Delete the cache
  if(cache_flag){
    deleteCache();
  }
  #pragma endregion

  /* Once you reach here, the thread join calls blocking in main will succeed and the program should terminate */
}
/**********************************************************************************/

/* ******************** Dynamic Pool Code  [Extra Credit A] **********************/
// Function implements the policy to change the worker thread pool dynamically
// depending on the number of requests
void * dynamic_pool_size_update(void *arg) {

  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  EnableThreadCancel(); //Allow thread to be asynchronously cancelled
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/ 

  /* TODO (dynamic.I)
  *    Description:      Setup any cleanup handler functions to release any locks and free any memory allocated in this function
  *    Hint:             pthread_cleanup_push(pthread_lock_release,  <address_to_lock>);
  *                      pthread_cleanup_push(pthread_mem_release,   <address_to_mem>);
  */
  #pragma region SOLUTION CODE --> Add signal handler for cancel signal
  pthread_cleanup_push(pthread_lock_release, &req_queue_mutex); //Add cleanup handler for when cancel is called to release lock resource
  #pragma endregion

  int id = -1;

  /* TODO (dynamic.II)
  *    Description:      Get the id as an input argument from arg, set it to ID
  */
  #pragma region SOLUTION CODE --> Get the input ID and add a helpful print
  id = *(int*)arg;
  #pragma endregion

  printf("%-30s [%3d] Started\n", "Dynamic Pool Thread", id);

  while(1) {
    /* TODO (dynamic.III)
    *    Description:      Run at regular intervals
    *                      Increase / decrease dynamically based on your policy
    *    Hint:             There will be limited help provided from TA's and professors here... Designed to be a challenge
    *                      Make sure this code is not run when the dynamic flag is 0
    */

    #pragma region SOLUTION CODE --> Extra Credit Cache Policy
    // (1) Wait a certain amount of time prior to updating thread pool
    usleep(5000);

    // (2) Lock the mutex to dynamically change thread pool
    if (pthread_mutex_lock(&req_queue_mutex) < 0) {
      	printf("Failed to lock queue! %s\n", strerror(errno));
        continue;
    }

    // (3) If there is less work than workers and we are not already at minimum workers --> Cancel some threads
    if (curr_queue_len < num_worker && num_worker != 1){
      int num_to_delete = num_worker - curr_queue_len - 1;
      for (int i = 0 ; (i < MAX_THREADS) && (curr_queue_len < num_worker) && (num_worker > 1) ; i++) {
        //Send a cancel signal to the worker thread
        if(pthread_cancel(worker_threads[i]) < 0){
          printf("ERROR: Could not kill worker thread [%ld] @ index [%d]\n", worker_threads[i], i);
          perror("could not kill\n");
          exit(-1);
        }

        //Reduce Worker Count
        num_worker--;
      }
      //Add an extra broadcast to all threads that the queue is not empty in case the joined threads failed to signal
      pthread_cond_broadcast(&req_queue_notempty);

      printf("Deleted %d worker threads because server load decreased\n", num_to_delete);

      //Need to release the lock before can join
      //(5) Release the queue mutex
      if (pthread_mutex_unlock(&req_queue_mutex) < 0) {
      	printf("Failed to unlock queue! %s\n", strerror(errno));

      for(int i = 0; i < num_to_delete; i++){
        //Join the worker thread
        if(pthread_join(worker_threads[i], NULL) < 0){
          printf("ERROR: Could not kill join thread [%ld] @ index [%d]\n", worker_threads[i], i);
          perror("could not join\n");
          exit(-1);
        }
      }
      printf("Waited %d worker threads because server load decreased\n", num_to_delete);
    }
    }
    // (4) If there is more work than workers --> Add some threads
    else if (curr_queue_len > num_worker){
      printf("Created %d worker threads because server load increased\n", curr_queue_len - num_worker);
      for (int i = 0 ; i < MAX_THREADS && curr_queue_len > num_worker ; i++) {
        pthread_create(worker_threads+i,NULL,worker,(void *)&id_arr[i]);
        num_worker++;
      }

      //(5) Release the queue mutex
      if (pthread_mutex_unlock(&req_queue_mutex) < 0) {
          printf("Failed to unlock queue! %s\n", strerror(errno));
      }
    }else{
      //(5) Release the queue mutex
      if (pthread_mutex_unlock(&req_queue_mutex) < 0) {
          printf("Failed to unlock queue! %s\n", strerror(errno));
      }
    }

    #pragma endregion
  }

  /* TODO (dynamic.IV)
  *    Description:      pop any cleanup handlers that were pushed onto the queue otherwise you will get compile errors
  *    Hint:             pthread_cleanup_pop(0);
  *                      Call pop for each time you call _push... the 0 flag means do not execute the cleanup handler after popping
  */
  #pragma region SOLUTION CODE --> Pop Cancel Signal Hanlder
  pthread_cleanup_pop(0);   //Pop cleanup handler #1 from top of queue and do not execute
  #pragma endregion

  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  return NULL;
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/ 
}
/**********************************************************************************/

/* ************************ Cache Code [Extra Credit B] **************************/

// Function to check whether the given request is present in cache
int getCacheIndex(char *request){
  /* TODO (GET CACHE INDEX)
  *    Description:      return the index if the request is present in the cache otherwise return INVALID
  */

  #pragma region SOLUTION CODE --> Get Cache Index
  for (int i = 0 ; i < cache_len ; i++){
    if (cache[i].request != NULL && strcmp(cache[i].request, request) == 0){
      return i;
    }
  }
  #pragma endregion

  return INVALID;    
}

// Function to add the request and its file content into the cache
void addIntoCache(char *mybuf, char *memory , int memory_size){
  /* TODO (ADD CACHE)
  *    Description:      It should add the request at an index according to the cache replacement policy
  *                      Make sure to allocate/free memory when adding or replacing cache entries
  */

  #pragma region SOLUTION CODE --> Add Into Cache
  // (1) Free request and content if not NULL in this location (aka remove from cache)
  if (cache[cache_evict_index].request != NULL){
    free(cache[cache_evict_index].request);
  }
  if (cache[cache_evict_index].content != NULL){
    free(cache[cache_evict_index].content);
  }

  // (2) Re-Allocate the request and conetent at this location
  cache[cache_evict_index].request = (char *)malloc(BUFF_SIZE * sizeof(char));
  cache[cache_evict_index].content = (char *)malloc(memory_size);

  // (3) Copy in memory and string into request and content
  strcpy(cache[cache_evict_index].request , mybuf);
  memcpy(cache[cache_evict_index].content , memory, memory_size);
  cache[cache_evict_index].len = memory_size;

  // (4) Update the cache eveiction index
  cache_evict_index = (cache_evict_index + 1) % cache_len;
  #pragma endregion

}

// Function to clear the memory allocated to the cache
void deleteCache(){
  /* TODO (CACHE)
  *    Description:      De-allocate/free the cache memory
  */
  #pragma region SOLUTION CODE --> Delete Cache
  for (int i = 0; i < cache_len; i++){
    free(cache[i].request);
    free(cache[i].content);
  }
  free(cache);
  #pragma endregion
}

// Function to initialize the cache
void initCache(){
  /* TODO (CACHE)
  *    Description:      Allocate and initialize an array of cache entries of length cache size
  */
  #pragma region SOLUTION CODE --> Init Cache
  cache = malloc(sizeof(cache_entry_t) * cache_len);
  for (int i = 0; i < cache_len; i++){
    cache[i].len = 0;
    cache[i].request = NULL;
    cache[i].content = NULL;
  }
  #pragma endregion

}

/**********************************************************************************/

/* ************************************ Utilities ********************************/
// Function to get the content type from the request
char* getContentType(char *mybuf) {
  /* TODO (Get Content Type)
  *    Description:      Should return the content type based on the file type in the request
  *                      (See Section 5 in Project description for more details)
  *    Hint:             Need to check the end of the string passed in to check for .html, .jpg, .gif, etc.
  */

  #pragma region SOLUTION CODE --> Get content type
  int requeue_len = strlen(mybuf);
  char * contenttype;
  if (strcmp(&mybuf[requeue_len-5],".html")==0) {
    contenttype = "text/html";
  } else if (strcmp(&mybuf[requeue_len-4],".jpg")==0) {
    contenttype = "image/jpeg";
  } else if (strcmp(&mybuf[requeue_len-4],".gif")==0) {
    contenttype = "image/gif";
  } else {
    contenttype = "text/plain";
  }
  return contenttype;
  #pragma endregion

  //TODO remove this line and return the actual content type
  //return NULL;
}

// Function to open and read the file from the disk into the memory
// Add necessary arguments as needed
int readFromDisk(int fd, char *mybuf, void **memory) {
  /* TODO (ReadFile.I)
  *    Description:      Try and open requested file, return INVALID if you cannot meaning error
  *    Hint:             Consider printing the file path of your request, it may be interesting and you might have to do something special with it before opening
  *                      If you cannot open the file you should return INVALID, which should be handeled by worker
  */

  #pragma region SOLUTION CODE --> Try and Open File
  int filefd;
  if ((filefd = open(mybuf+1,O_RDONLY)) == -1) { // it is mybuf + 1 because get request will return the path with a '/' in the front
    return INVALID;
  }
  #pragma endregion
  
  /* TODO (ReadFile.II)
  *    Description:      Find the size of the file you need to read, read all of the contents into a memory location and return the file size
  *    Hint:             Using fstat or fseek could be helpful here
  *                      What do we do with files after we open them?
  */ 
  #pragma region SOLUTION CODE --> Try and Open File
  struct stat st;
  fstat(filefd,&st);
  int filesize = st.st_size;
  *memory = malloc(filesize);
  read(filefd,*memory,filesize);
  close(filefd);
  return filesize;
  #pragma endregion
  

  //TODO remove this line and follow directions above
  //return INVALID;
}

/**********************************************************************************/

// Function to receive the request from the client and add to the queue
void * dispatch(void *arg) {

  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  EnableThreadCancel();                                         //Allow thread to be asynchronously cancelled
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/ 

  /* TODO (B.I)
  *    Description:      Setup any cleanup handler functions to release any locks and free any memory allocated in this function
  *    Hint:             pthread_cleanup_push(pthread_lock_release,  <address_to_lock>);
  *                      pthread_cleanup_push(pthread_mem_release,   <address_to_mem>);   [If you are putting memory in the cache, who free's it? answer --> cache delete]
  */
  #pragma region SOLUTION CODE --> Add signal handler for cancel signal
  pthread_cleanup_push(pthread_lock_release, &log_mutex);         //Add cleanup handler for when cancel is called to release lock resource
  #pragma endregion

  /* TODO (B.II)
  *    Description:      Get the id as an input argument from arg, set it to ID
  */
  int id = -1;

  #pragma region SOLUTION CODE --> Get the input ID and add a helpful print
  id = *(int*)arg;
  #pragma endregion

  printf("%-30s [%3d] Started\n", "Dispatcher", id);


  while (1) {

    /* TODO (B.INTERMEDIATE SUBMISSION)
    *    Description:      Receive a single request and print the conents of that request
    *                      The TODO's below are for the full submission, you do not have to use a 
    *                      buffer to receive a single request 
    *    Hint:             Helpful Functions: int accept_connection(void) | int get_request(int fd, char *filename
    *                      Recommend using the request_t structure from server.h to store the request
    *                      Print the request information using a command like this: 
    *                      printf(“Dispatcher Received Request: fd[%d] request[%s]\n”, <insert_fd>, <insert_str>); 
    */

    //NOT NEEDED FOR FINAL SUBMISSION --> Skipped in Solution

    /* TODO (B.III)
    *    Description:      Accept client connection
    *    Utility Function: int accept_connection(void) //utils.h => Line 24
    *    Hint:             What should happen if accept_connection returns less than 0?
    */
    #pragma region SOLUTION CODE --> Accept Client Connection
    int fd = accept_connection();
    if (fd < 0) { return NULL; }
    #pragma endregion

    /* TODO (B.IV)
    *    Description:      Get request from the client
    *    Utility Function: int get_request(int fd, char *filename); //utils.h => Line 41
    *    Hint:             What should happen if get_request does not return 0?
    */
    #pragma region SOLUTION CODE --> Get a Request
    char mybuf[BUFF_SIZE];
    if (get_request(fd,mybuf) != 0) { continue; /*back to top of loop!*/ }
    #pragma endregion

    /* TODO (B.V)
    *    Description:      Add the request into the queue
    *    Hint:             Utilize the request_t structure in server.h...
    *                      How can you safely add a request to somewhere that other threads can also access? 
    *                      Probably need some synchronization and some global memory... 
    *                      You cannot add onto a full queue... how should you check this? 
    */
    #pragma region SOLUTION CODE --> Get a Request
    //(1) Copy the filename from get_request into allocated memory to put on request queue
    char *reqptr = (char *)malloc(strlen(mybuf)+1);
    if (reqptr == NULL) {
        printf("Failed to allocate memory: %s", strerror(errno));
    }
    strcpy(reqptr,mybuf);

    //(2) Request thread safe access to the request queue
    if (pthread_mutex_lock(&req_queue_mutex) < 0) {
      printf("Failed to lock queue! %s\n", strerror(errno));
    }

    //(3) Check for a full queue... wait for an empty one which is signaled from req_queue_notfull
    while (curr_queue_len == queue_len) { // the queue is full!
        pthread_cond_wait(&req_queue_notfull,&req_queue_mutex);
    }

    //(4) Insert the request into the queue
    requests[req_insert_index].fd       = fd;
    requests[req_insert_index].request  = reqptr;

    //(5) Update the queue index in a circular fashion
    curr_queue_len++;
    req_insert_index = (req_insert_index + 1) % queue_len;

    //(6) Release the lock on the request queue and signal that the queue is not empty anymore
    pthread_cond_signal(&req_queue_notempty);
    if (pthread_mutex_unlock(&req_queue_mutex) < 0) {
      printf("Failed to unlock queue! %s\n", strerror(errno));
    }
    #pragma endregion
  }

  /* TODO (B.VI)
  *    Description:      pop any cleanup handlers that were pushed onto the queue otherwise you will get compile errors
  *    Hint:             pthread_cleanup_pop(0);
  *                      Call pop for each time you call _push... the 0 flag means do not execute the cleanup handler after popping
  */
  #pragma region SOLUTION CODE --> Pop Cancel Signal Hanlder
  pthread_cleanup_pop(0);   //Pop cleanup handler #1 from top of queue and do not execute
  #pragma endregion

  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  return NULL;
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/ 
}

/**********************************************************************************/

// Function to retrieve the request from the queue, process it and then return a result to the client
void * worker(void *arg) {
  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  EnableThreadCancel();                                         //Allow thread to be asynchronously cancelled
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/

  // Helpful/Suggested Declaration
  int num_request = 0;              //Integer for tracking each request for printing into the log
  bool cache_hit  = false;          //Boolean flag for tracking cache hits or misses if doing [Extra Credit B]
  int filesize    = 0;              //Integer for holding the file size returned from readFromDisk or the cache
  void *memory    = NULL;           //memory pointer where contents being requested are read and stored
  int fd          = INVALID;        //Integer to hold the file descriptor of incoming request
  char mybuf[BUFF_SIZE];            //String to hold the file path from the request

  /* TODO (C.I)
  *    Description:      Setup any cleanup handler functions to release any locks and free any memory allocated in this function
  *    Hint:             pthread_cleanup_push(pthread_lock_release,  <address_to_lock>);
  *                      pthread_cleanup_push(pthread_mem_release,   <address_to_mem>);   [If you are putting memory in the cache, who free's it? answer --> cache delete]
  */
  #pragma region SOLUTION CODE --> Add signal handler for cancel signal
  pthread_cleanup_push(pthread_lock_release, &req_queue_mutex);     //Add cleanup handler for when cancel is called to release lock resource
  pthread_cleanup_push(pthread_lock_release, &cache_mutex);         //Add cleanup handler for when cancel is called to release lock resource
  pthread_cleanup_push(pthread_lock_release, &log_mutex);           //Add cleanup handler for when cancel is called to release lock resource
  pthread_cleanup_push(pthread_mem_release, memory);                //Add cleanup handler for when cancel is called to release memory
  #pragma endregion

  /* TODO (C.II)
  *    Description:      Get the id as an input argument from arg, set it to ID
  */  
  int id = -1;

  #pragma region SOLUTION CODE --> Get the input ID and add a helpful print
  id = *(int*)arg;
  #pragma endregion

  printf("%-30s [%3d] Started\n", "Worker", id);

  while (1) {
    /* TODO (C.III)
    *    Description:      Get the request from the queue
    *    Hint:             You will need thread safe access to the queue... how?
    *                      How will you handle an empty queue? How can you tell dispatch the queue is open? 
    *                      How will you index into the request queue? Global variable probably... How will you update your request queue index?
    *                      IMPORTANT... if you are processing a request you cannot be cancelled... how do you block being cancelled? (see BlockCancelSignal()--> server.h) 
    *                      IMPORTANT... if you are blocking the cancel signal... when do you re-enable it?
    */
    #pragma region SOLUTION CODE --> Get a request from the queue
    //(1) Request thread safe access to the request queue by getting the req_queue_mutex lock
    if (pthread_mutex_lock(&req_queue_mutex) < 0) {
      printf("Failed to lock! %s\n", strerror(errno));
      continue;
    }

    //(2) While the request queue is empty conditionally wait for the request queue lock once the not empty signal is raised
    while (curr_queue_len == 0) { // the queue is empty!
      if (pthread_cond_wait(&req_queue_notempty,&req_queue_mutex) < 0) {
        printf("Failed to wait! %s\n", strerror(errno));
      }
    }

    //(3) Since we are pulling from the queue we need to service the request before we can be exited
    BlockCancelSignal();

    //(4) Now that you have the lock AND the queue is not empty, read from the request queue
    fd = requests[req_remove_index].fd;
    
    strcpy(mybuf,requests[req_remove_index].request);
    free(requests[req_remove_index].request);

    //(5) Update the request queue remove index in a circular fashion
    curr_queue_len--;
    req_remove_index = (req_remove_index + 1) % queue_len; // ring buffer

    //(6) Check for a path with only a / if that is the case add index.html to it
    if (strcmp(mybuf,"/") == 0){
      strcpy(mybuf,"/index.html");
    }

    //(7) Fire the request queue not full signal to indicate the queue has a slot opened up and release the request queue lock
    if (pthread_cond_signal(&req_queue_notfull) < 0) {
      printf("Failed to signal! %s\n", strerror(errno));
    }
    if (pthread_mutex_unlock(&req_queue_mutex) < 0) {
      printf("Failed to unlock! %s\n", strerror(errno));
    }
    #pragma endregion

    /* TODO (C.IV)
    *    Description:      Get the data from the disk or the cache (extra credit B)
    *    Local Function:   int readFromDisk(//necessary arguments//);
    *                      int getCacheIndex(char *request);  //[Extra Credit B]
    *                      void addIntoCache(char *mybuf, char *memory , int memory_size);  //[Extra Credit B]
    */
    #pragma region SOLUTION CODE --> Read the data from disk or check the cache

    // (1) Check for the use of the cache flag
    if(cache_flag){
      
      // (1.a) Acquire a lock to the cache
      if (pthread_mutex_lock(&cache_mutex) < 0) {
        printf("Failed to lock cache mutex! %s\n", strerror(errno));
      }

      // (1.b) Check the cache index
      int index = getCacheIndex(mybuf);
      if(index != INVALID){

        //  (1.c) If index is invalid, then get the filesize and copy out the memory
        cache_hit = true;
        filesize  = cache[index].len;
        memory    = malloc(filesize);
        memcpy(memory, cache[index].content, filesize);

      }else{

        // (1.d) If index is NOT valid, read file from disk, check for valid read and then add to cache
        cache_hit = false;
        filesize  = readFromDisk(fd, mybuf, &memory);

        if(filesize == -1) {
          //If readFromDisk error, release lock and continue on while loop
          if (pthread_mutex_unlock(&cache_mutex) < 0) {
            printf("Failed to unlock cache mutex! %s\n", strerror(errno));
          }
        }else{
          addIntoCache(mybuf, memory, filesize);
        }
      }

      // (1.e) Release the cache lock
      if (pthread_mutex_unlock(&cache_mutex) < 0) {
        printf("Failed to unlock cache mutex! %s\n", strerror(errno));
      }
    }else{
      // (2.a) If not using the cache read directly from the disk into memory
      filesize  = readFromDisk(fd, mybuf, &memory);
    }
    #pragma endregion


    /* TODO (C.V)
    *    Description:      Log the request into the file and terminal
    *    Utility Function: LogPrettyPrint(FILE* to_write, int threadId, int requestNumber, int file_descriptor, char* request_str, int num_bytes_or_error, bool cache_hit);
    *    Hint:             Call LogPrettyPrint with to_write = NULL which will print to the terminal
    *                      You will need to lock and unlock the logfile to write to it in a thread safe manor
    */
    #pragma region SOLUTION CODE --> Print to the Log File and Terminal
    //Lock logfile
    if (pthread_mutex_lock(&log_mutex) < 0) {
      printf("Failed to lock log mutex! %s\n", strerror(errno));
    }

    //Write to logfile and terminal
    LogPrettyPrint(logfile, id, ++num_request, fd, mybuf, filesize, cache_hit);
    LogPrettyPrint(NULL, id, num_request, fd, mybuf, filesize, cache_hit);

    //Unlock logfile
    if (pthread_mutex_unlock(&log_mutex) < 0) {
      printf("Failed to unlock log mutex! %s\n", strerror(errno));
    }
    #pragma endregion

    /* TODO (C.VI)
    *    Description:      Get the content type and return the result or error
    *    Utility Function: (1) int return_result(int fd, char *content_type, char *buf, int numbytes); //utils.h => Line 63
    *                      (2) int return_error(int fd, char *buf); //utils.h => Line 75
    *    Hint:             Don't forget to free your memory and set it to NULL so the cancel hanlder does not double free
    *                      You need to focus on what is returned from readFromDisk()... if this is invalid you need to handle that accordingly
    *                      This might be a good place to re-enable the cancel signal... EnableThreadCancel() [hint hint]
    */
    #pragma region SOLUTION CODE --> Check for error, retrieve content type, return result
    if(filesize == INVALID){
      return_error(fd,"ERROR processing request. See Log");
    }else{
      char *contenttype = getContentType(mybuf);
      if (return_result(fd,contenttype,memory,filesize) != 0) {
        printf("Couldn't return result, thread id=%d\n",id);
      }

      free(memory);
      memory = NULL;
    }

    //Since we have servied the request we can be cancelled again
    EnableThreadCancel();
    #pragma endregion
  }

  /* TODO (C.VII)
  *    Description:      pop any cleanup handlers that were pushed onto the queue otherwise you will get compile errors
  *    Hint:             pthread_cleanup_pop(0);
  *                      Call pop for each time you call _push... the 0 flag means do not execute the cleanup handler after popping
  */
  #pragma region SOLUTION CODE --> Pop Cancel Signal Hanlder
  pthread_cleanup_pop(0);   //Pop cleanup handler #1 from top of queue and do not execute
  pthread_cleanup_pop(0);   //Pop cleanup handler #2 from top of queue and do not execute
  pthread_cleanup_pop(0);   //Pop cleanup handler #3 from top of queue and do not execute
  pthread_cleanup_pop(0);   //Pop cleanup handler #4 from top of queue and do not execute
  #pragma endregion

  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  return NULL;
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/
}

/**********************************************************************************/

int main(int argc, char **argv) {
  /********************* DO NOT REMOVE SECTION - TOP     *********************/

  // Error check on number of arguments
  if(argc != 9){
    printf("usage: %s port path num_dispatcher num_workers dynamic_flag cache_flag queue_length cache_size\n", argv[0]);
    return -1;
  }

  int port            = -1;
  char path[PATH_MAX] = "no path set\0";
  num_dispatcher      = -1;                 //global variable
  num_worker          = -1;                 //global variable
  dynamic_flag        = 99999;              //global variable
  cache_flag          = 99999;              //global variable
  queue_len           = -1;                 //global variable
  cache_len           = -1;                 //global variable

  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/

  /* TODO (A.I)
  *    Description:      Get the input args --> (1) port (2) path (3) num_dispatcher (4) num_workers (5) dynamic_flag (6) cache_flag (7) queue_length (8) cache_size
  */
  #pragma region SOLUTION CODE --> Extract Input Arguments
  port            = atoi(argv[PORT_INDEX]);
  memset(path, '\0', PATH_MAX);
  strcpy(path, argv[PATH_INDEX]);
  num_dispatcher  = atoi(argv[NUM_DSP_INDEX]);
  num_worker      = atoi(argv[NUM_WRK_INDEX]);
  dynamic_flag    = atoi(argv[DYNM_FLAG_INDEX]);
  cache_flag      = atoi(argv[CACHE_FLAG_INDEX]);
  queue_len       = atoi(argv[QUEUE_LEN_INDEX]);
  cache_len       = atoi(argv[CACHE_LEN_INDEX]);
  #pragma endregion

  /* TODO (A.II)
  *    Description:     Perform error checks on the input arguments
  *    Hints:           (1) port: {Should be >= MIN_PORT and <= MAX_PORT} | (2) path: {Consider checking if path exists (or wil be caught later)} 
  *                     (3) num_dispatcher: {Should be >= 1 and <= MAX_THREADS} | (4) num_workers: {Should be >= 1 and <= MAX_THREADS}
  *                     (5) dynamic_flag: {Should be 1 or 0} | (6) cache_flag: {Should be 1 or 0} | (7) queue_length: {Should be >= 1 and <= MAX_QUEUE_LEN}
  *                     (8) cache_size: {Should be >= 1 and <= MAX_CE}
  */

  #pragma region SOLUTION CODE --> Verify Input Arguments
  if(port < MIN_PORT || port > MAX_PORT){
    printf("ERROR: Port [%d] was not between [%d <--> %d]\n", port, MIN_PORT, MAX_PORT);
    return -1;
  }
  DIR* dir = opendir(path);
  if (dir)
    closedir(dir);
  else{
    printf("ERROR: Path [%s] could not be opened, likey did not exist\n", path);
    return -1;
  }

  if(num_dispatcher < 1 || num_dispatcher > MAX_THREADS){
    printf("ERROR: Number of Dispatchers [%d] was not between [%d <--> %d]\n", num_dispatcher, 1, MAX_THREADS);
    return -1;
  }

  if(num_worker < 1 || num_worker > MAX_THREADS){
    printf("ERROR: Number of Workers [%d] was not between [%d <--> %d]\n", num_worker, 1, MAX_THREADS);
    return -1;
  }

  if(dynamic_flag != 1 && dynamic_flag != 0){
    printf("ERROR: Dynamic Flag [%u] was not 0 or 1\n", dynamic_flag);
    return -1;
  }

  if(cache_flag != 1 && cache_flag != 0){
    printf("ERROR: Cache Flag [%u] was not 0 or 1\n", cache_flag);
    return -1;
  }

  if(queue_len < 1 || queue_len > MAX_QUEUE_LEN){
    printf("ERROR: Queue length [%d] was not between [%d <--> %d]\n", queue_len, 1, MAX_QUEUE_LEN);
    return -1;
  }

  if(cache_len < 1 || cache_len > MAX_CE){
    printf("ERROR: Cache length [%d] was not between [%d <--> %d]\n", cache_len, 1, MAX_CE);
    return -1;
  }
  #pragma endregion

  /********************* DO NOT REMOVE SECTION - TOP    *********************/
  printf("Arguments Verified:\n\
    Port:           [%d]\n\
    Path:           [%s]\n\
    num_dispatcher: [%d]\n\
    num_workers:    [%d]\n\
    dynamic_flag:   [%s]\n\
    cache_flag:     [%s]\n\
    queue_length:   [%d]\n\
    cache_size:     [%d]\n\n", port, path, num_dispatcher, num_worker, dynamic_flag ? "TRUE" : "FALSE", cache_flag ? "TRUE" : "FALSE", queue_len, cache_len);
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/

  /* TODO (A.III)
  *    Description:      Change SIGINT action for graceful termination
  *    Hint:             Implement gracefulTerminationHandler(), use global "struct sigaction action", see lab 8 for signal handlers
  */
  #pragma region SOLUTION CODE --> Install Signal Hanlder for Graceful Termination
  action.sa_handler=gracefulTerminationHandler;
  action.sa_flags=0;
  sigemptyset(&action.sa_mask);
  if(sigaction(SIGINT,&action,NULL) < 0){
    perror("ERROR: Could not install signal handler\n");
    return -2;
  }
  #pragma endregion

  /* TODO (A.IV)
  *    Description:      Open log file
  *    Hint:             Use Global "File* logfile", use LOG_FILE_NAME as the name, what open flags do you want?
  */
  #pragma region SOLUTION CODE --> Open Logfile
  logfile = fopen(LOG_FILE_NAME,"w");
  if(logfile == NULL){
    perror("ERROR: Could not open logfile\n");
    return -2;
  }
  #pragma endregion

  /* TODO (A.V)
  *    Description:      Change the current working directory to server root directory
  */
  #pragma region SOLUTION CODE --> Change Directory
  if(chdir(path) != 0){
    perror("ERROR: Could not change directory into path");
    return -2;
  }
  #pragma endregion

  /* TODO (A.VI)
  *    Description:      Initialize cache (IF CACHE FLAG SET) (extra credit B)
  *    Local Function:   void    initCache();
  */
  #pragma region SOLUTION CODE --> Init Cache
  if(cache_flag){
    initCache();
  }
  #pragma endregion

  /* TODO (A.VII)
  *    Description:      Start the server
  *    Utility Function: void init(int port); //utils.h => Line 14
  */
  #pragma region SOLUTION CODE --> Init Cache
  init(port);
  #pragma endregion

  /* TODO (A.VIII)
  *    Description:      Create dispatcher and worker threads (all threads should be detachable)
  *    Hints:            Use pthread_create, you will want to store pthread's globally
  *                      You will want to initialize some kind of global array to tell threads when to exit
  */
  #pragma region SOLUTION CODE --> Create the dispatcher and worker threads
  for (int i = 0; i < num_worker; i++) {
    id_arr[i] = i;
  }
  for (int i = 0; i < num_worker; i++) {
    pthread_create(worker_threads+i,NULL,worker,(void *)&id_arr[i]);
  }
  for (int i = 0; i < num_dispatcher; i++) {
    pthread_create(dispatch_threads+i,NULL,dispatch,(void *)&id_arr[i]);
  }
  #pragma endregion

  /* TODO (A.IX)
  *    Description:      Create dynamic pool manager thread (IF DYNAMIC FLAG SET) [Extra Credit A]
  *    Hint:             Dynamic pool manager is a thread, it's ID should be DYNAM_POOL_TID
  *                      How should you track this p_thread so you can terminate it later? [use]
  */
  #pragma region SOLUTION CODE --> Create Dynamic Pool Manager Thread
  if(dynamic_flag){
    int local_id = DYNAM_POOL_TID;
    pthread_create(&dynamic_pool_deamon, NULL, dynamic_pool_size_update, (void *)&(local_id));
  }
  #pragma endregion


  /* TODO (A.X)
  *    Description:      Wait for each of the threads to complete their work
  *    Hint:             What can you call that will wait for threads to exit? How can you get threads to exit from ^C (or SIGINT)
  *                      If you are using the dynamic pool flag, you should wait for that thread to exit too
  */
  #pragma region SOLUTION CODE --> Wait for Threads to be joined
  for (int i = 0; i < num_dispatcher; i++) {
    if(pthread_join(dispatch_threads[i],NULL) < 0){
      printf("ERROR: Could not join dispatcher thread [%ld] @ index [%d]\n", dispatch_threads[i], i);
      perror("could not join\n");
      return -1;
    }
  }

  for (int i = 0; i < num_worker; i++) {
    if(pthread_join(worker_threads[i],NULL) < 0){
      printf("ERROR: Could not join worker thread [%ld] @ index [%d]\n", worker_threads[i], i);
      perror("could not join\n");
      return -1;
    }
  }

  if(dynamic_flag){
    if(pthread_join(dynamic_pool_deamon, NULL) < 0){
        printf("ERROR: Could not join dynamic pool thread [%ld]\n", dynamic_pool_deamon);
        perror("could not join\n");
        return -1;
    }
  }
  #pragma endregion

  /* SHOULD NOT HIT THIS CODE UNLESS RECEIVED SIGINT AND THREADS CLOSED */
  /********************* DO NOT REMOVE SECTION - TOP     *********************/
  printf("web_server closing, exiting main\n");
  fflush(stdout);
  return 0;
  /********************* DO NOT REMOVE SECTION - BOTTOM  *********************/  
}
