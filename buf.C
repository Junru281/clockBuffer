#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
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

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

const Status BufMgr::allocBuf(int &frame)
{
    int allocBufcount=0; 
    advanceClock();
    //move clockhand to next frame
    Status WStatus = OK;
    //record writing to disk, if it's OK all the time then there's no I/O error

    do{
    if (bufTable[clockHand].valid==false) {
        frame = clockHand; 
        //current frame
        bufTable[frame].Clear(); 
        //clear the current frame
        return OK;
    }
    //first examine if it's valid, if not clear it

        if (bufTable[clockHand].refbit==true) {
            bufTable[clockHand].refbit = false;
            advanceClock();
            //move clockhand to next frame
        } 
        //change true refbit for next round
        else {
            if (bufTable[clockHand].pinCnt==0) {
                 // if nothing is using this page
                 if (bufTable[clockHand].dirty) {
                    WStatus = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                    // write to disk
                    if (WStatus==UNIXERR)
                        return UNIXERR;
                        // if I/O not successful return UNIXERR(defined in error.h)
                 }

                hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                // remove page from hashtable                
                frame = clockHand;
                //current frame
                bufTable[frame].Clear();
                //clear it
                return OK;
            }
        }       
    } while (allocBufcount++ < numBufs);
    //allocBufcount+1 for every circulation, until it is bigger than numBufs(this round is over)
    
    return BUFFEREXCEEDED;
   //if we got into this line it means that all the pages are pinned(pinCnt>0) or all refbits are 1
   //return BUFFEREXCEEDED(defined in error.h)
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    Status hashStatus = OK;
    Status allocStatus = OK;
    Status fileStatus = OK;
    Status insertStatus = OK;
    int frameNo = 0;
    hashStatus = hashTable->lookup(file, PageNo, frameNo);
    // Case 1) Page is not in the buffer pool.  
    if (hashStatus == HASHNOTFOUND)
    {
        // Call allocBuf() to allocate a buffer frame
        allocStatus = allocBuf(frameNo);
        // BUFFEREXCEEDED if all pages have been pinned/actively used
        if (allocStatus == BUFFEREXCEEDED) {
            return BUFFEREXCEEDED;  
        }
        // UNIXERR if a Unix error occurred
        else if (allocStatus == UNIXERR){
            return UNIXERR;  
        }
        else {
            // call the method file->readPage() to read the page from disk into the buffer pool frame.     
            fileStatus = file->readPage(PageNo, &bufPool[frameNo]);
            if(fileStatus != OK){
                return fileStatus;
            }
            // Finally, invoke Set() on the frame to set it up properly. 
            // Set() will leave the pinCnt for the page set to 1.  
            bufTable[frameNo].Set(file, PageNo);    
            insertStatus = hashTable->insert(file, PageNo, frameNo);
            // HASHTBLERROR if a hash table error occurred.
            if(insertStatus == HASHTBLERROR) {
                return HASHTBLERROR;
            }else {
                // Return a pointer to the frame containing the page via the page parameter.    
                page = &bufPool[frameNo];
            }
        }
    }
    // Case 2) Page is in the buffer pool.  
    else if (hashStatus == OK)
    {
        // get the frame metadata
        BufDesc *tmpbuf = &(bufTable[frameNo]);
        // In this case set the appropriate refbit
        tmpbuf->refbit = true;
        // increment the pinCnt for the page
        tmpbuf->pinCnt += 1;
        // return a pointer to the frame containing the page via the page parameter.
        page = &bufPool[frameNo];
    }
    return OK;
}

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    // Check if the page is in the buffer table
    if (status == HASHNOTFOUND)
    {
        return status;
    }
    // Checks if page is pinned
    if (bufTable[frameNo].pinCnt == 0)
    {
        return PAGENOTPINNED;
    }
    // decrement pin count
    bufTable[frameNo].pinCnt--;


    if (dirty)
    {
        bufTable[frameNo].dirty = true;
    }
    return OK;


}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    //Allocate a new page in the file, updating the page number reference
    file->allocatePage(pageNo);
    int frameNo = 0;

    Status Status_Alloc = allocBuf(frameNo);

    if (Status_Alloc == BUFFEREXCEEDED)
    {
        return BUFFEREXCEEDED;
    }
    else if (Status_Alloc == UNIXERR)
    {
        return UNIXERR;
    }

    //Initialize the frame in the buffer table for the allocated page
    page = &bufPool[frameNo];

    bufTable[frameNo].Set(file, pageNo);

    //Insert the new page-frame mapping into the hash table
    Status Status_Insert = OK;
    Status_Insert = hashTable->insert(file, pageNo, frameNo);
    
    if (Status_Insert == HASHTBLERROR)
    {
        return HASHTBLERROR;
    }

    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
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

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

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
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
