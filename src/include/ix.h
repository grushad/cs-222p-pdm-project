#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IXPageManager{
    public:
        bool checkLeaf(void* data){
            auto* dataC = static_cast<unsigned char*>(data);
            unsigned res;
            memcpy(&res,dataC + PAGE_SIZE - 1, 1);
            return res == 1;
        }
        unsigned calcNumEntries(void* data){
            auto* dataC = static_cast<unsigned char*>(data);
            unsigned res;
            memcpy(&res,dataC + PAGE_SIZE - 3, 2);
            return res;
        }
        unsigned calcFreeBytes(void* data){
            auto* dataC = static_cast<unsigned char*>(data);
            unsigned res;
            memcpy(&res,dataC + PAGE_SIZE - 5, 2);
            return res;
        }
        PageNum calcRightSibling(void* data){
            auto* dataC = static_cast<unsigned char*>(data);
            PageNum rightSibling;
            memcpy(&rightSibling,dataC + PAGE_SIZE - 9, UNSIGNED_SZ);
            return rightSibling;
        }
        IXPageManager(void* data){
            this->isLeaf = checkLeaf(data);
            this->numEntries = calcNumEntries(data);
            this->freeBytes = calcFreeBytes(data);
            this->rightSibling = calcRightSibling(data);
        }
        bool getIsLeaf(){
            return this->isLeaf;
        }
        unsigned getNumEntries(){
            return this->numEntries;
        }
        unsigned getFreeBytes(){
            return this->freeBytes;
        }
        PageNum getRightSibling(){
            return this->rightSibling;
        }

        RC getKeys(const unsigned &key, std::vector<unsigned> &keyList){

        }

    protected:
        bool isLeaf;
        unsigned numEntries;
        unsigned freeBytes;
        PageNum rightSibling;
    };

    class IXRecordManager{
    public:
        IXRecordManager(const Attribute &indexAttr, void* data){
            this->len = UNSIGNED_SZ;
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2){
                //varchar type
                memcpy(&len,dataC,UNSIGNED_SZ);
                dataC += UNSIGNED_SZ;
            }
            memcpy(this->key,dataC,len);
        }
    protected:
        unsigned len;
        void* key;
    };

    class IXLeafRecordManager:public IXRecordManager{
    public:
        IXLeafRecordManager(const Attribute &indexAttr, void* data)
                :IXRecordManager(indexAttr,data){
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2)
                dataC += UNSIGNED_SZ;
            memcpy(&this->numRIDs, dataC + this->len, UNSIGNED_SZ);
            dataC += len + UNSIGNED_SZ;
            for(int i = 0; i < this->numRIDs; i++){
                RID rid;
                memcpy(&rid.pageNum,dataC, UNSIGNED_SZ);
                memcpy(&rid.slotNum,dataC + UNSIGNED_SZ, UNSIGNED_SZ / 2);
                this->RIDList.push_back(rid);
                dataC += UNSIGNED_SZ + (UNSIGNED_SZ / 2);
            }
        }
    protected:
        unsigned numRIDs;
        std::vector<RID> RIDList;
    };

    class IXIndexRecordManager:public IXRecordManager{
    public:
        IXIndexRecordManager(const Attribute &indexAttr, void* data)
            : IXRecordManager(indexAttr,data){
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2)
                dataC += UNSIGNED_SZ;
            memcpy(&this->leftChild,dataC + this->len,UNSIGNED_SZ);
        }
    protected:
        PageNum leftChild;
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        FileHandle fileHandle;
        PageNum root;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };
}// namespace PeterDB
#endif // _ix_h_
