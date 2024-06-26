#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    // out << "allocbuf" << endl;
    advanceClock();
    int count = 0;
    BufDesc * tempbuf = &(bufTable[clockHand]);
    Status status;

    while(count < numBufs * 2){
        // cout << "while loop" << endl;
        if(bufTable[clockHand].valid == false){
            // cout << "validity check" << endl;
            frame = clockHand;
            return OK;
            // cout << "check return" << endl;
        }
        else if(bufTable[clockHand].refbit == true){
            // cout << "ref check" << endl;
            bufTable[clockHand].refbit = false;
            advanceClock();
            tempbuf = &(bufTable[clockHand]);
            count++;
        }
        else{
            if(bufTable[clockHand].pinCnt > 0){
                // cout << "pin check" << endl;
                advanceClock();
                tempbuf = &(bufTable[clockHand]);
                count++;
            }
            else{
                if(bufTable[clockHand].dirty == true){
                    //flush
                    if ((status = tempbuf->file->writePage(tempbuf->pageNo, &(bufPool[clockHand]))) != OK)
	                    return status;
                }
                //clear from hashtable
                hashTable->remove(tempbuf->file,tempbuf->pageNo);
                frame = clockHand;
                return OK;
            }
        }
    }

    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{   
    int frame;
    Status status;
    if(hashTable -> lookup(file, PageNo, frame) == OK){
        bufTable[frame].refbit = true;
        bufTable[frame].pinCnt++;
        page = &bufPool[frame];
        return OK;
    }
    else{
        if((status = allocBuf(frame)) != OK) {
            return status;
        }
        if ((status = file->readPage(PageNo, &bufPool[frame])) != OK) {
            return status;
        }
        //insert page to hashtable
        if(hashTable->insert(file, PageNo, frame) != OK)
            return HASHTBLERROR;
        bufTable[frame].Set(file, PageNo);
        page = &bufPool[frame];
        return OK;
    }
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    // cout << "unpin page" << endl;
    int frame;
    if(hashTable -> lookup(file, PageNo, frame) != OK){
        return HASHNOTFOUND;
    }

    if(bufTable[frame].pinCnt <= 0){
            return PAGENOTPINNED;
    }
    else{
        bufTable[frame].pinCnt--;
    }
    

    if(dirty){
        bufTable[frame].dirty = true;
    }
    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{   
    int frame;
    Status status;
    file->allocatePage(pageNo);
    if((status = allocBuf(frame)) != OK)
        return status;
    // cout << "after allocbuf" << endl;
    if(hashTable->insert(file, pageNo, frame) != OK)
        return HASHTBLERROR;
    // cout << "after hashtable insert" << endl;
    bufTable[frame].Set(file, pageNo);
    page = &bufPool[frame];
    // cout << "after set" << endl;
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


