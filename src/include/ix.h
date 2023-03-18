#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstring>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {

    class IXPageManager;
    class IXRecordManager;
    class IXLeafRecordManager;
    class IXIndexRecordManager;

    class IX_ScanIterator;
    class IXFileHandle;


    typedef struct IntKey{
        unsigned key;
        unsigned offset;
    }IntKey;

    typedef struct FloatKey{
        float key;
        unsigned offset;
    }FloatKey;

    typedef struct SKey{
        char* key;
        unsigned offset;
    }SKey;

    class IXRecordManager{
    public:
        IXRecordManager(const Attribute &indexAttr, void* data){
            this->len = UNSIGNED_SZ;
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2){
                memcpy(&len,dataC,UNSIGNED_SZ);
                dataC += UNSIGNED_SZ;
            }
            this->key = calloc(1,len);
            memcpy(this->key,dataC,len);
        }
        unsigned getRecordLen(){
            return this->recordLen;
        }
        void* getKey(){
            return this->key;
        }
        unsigned getKeyLen(){
            return this->len;
        }
        IXRecordManager();
    protected:
        unsigned len;
        void* key;
        unsigned recordLen;
    };

    class IXLeafRecordManager:public IXRecordManager{
    public:
        IXLeafRecordManager(const Attribute &indexAttr, void* data)
                :IXRecordManager(indexAttr,data){
            this->recordLen = 0;
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2){
                dataC += UNSIGNED_SZ; //len of varchar key
                this->recordLen += UNSIGNED_SZ;
            }
            memcpy(&this->numRIDs, dataC + this->len, UNSIGNED_SZ);
            dataC += this->len + UNSIGNED_SZ; //key + numRIDs
            this->recordLen += len + UNSIGNED_SZ;
            for(int i = 0; i < this->numRIDs; i++){
                RID rid;
                memcpy(&rid.pageNum,dataC, UNSIGNED_SZ);
                memcpy(&rid.slotNum,dataC + UNSIGNED_SZ, UNSIGNED_SZ / 2);
                this->RIDList.push_back(rid);
                dataC += UNSIGNED_SZ + (UNSIGNED_SZ / 2);
                this->recordLen += UNSIGNED_SZ + (UNSIGNED_SZ / 2);
            }
        }
        unsigned getNumRIDs(){
            return this->numRIDs;
        }
        std::vector<RID> getRIDList(){
            return this->RIDList;
        }

        unsigned getRIDOffset(const Attribute &indexAttr, RID rid, char* data){
            unsigned currOff = UNSIGNED_SZ * 2; //key length (in case of int ,float) + num Rids field
            if(indexAttr.type == 2){
                currOff += this->getKeyLen();
            }
            data += currOff;
            for(int i = 0; i < this->numRIDs; i++){
                RID cur;
                memcpy(&cur.pageNum,data, UNSIGNED_SZ);
                memcpy(&cur.slotNum,data + UNSIGNED_SZ, UNSIGNED_SZ / 2);
                if(cur.pageNum == rid.pageNum && cur.slotNum == rid.slotNum){
                    return currOff;
                }
                currOff += UNSIGNED_SZ + (UNSIGNED_SZ / 2);
                data += UNSIGNED_SZ + (UNSIGNED_SZ / 2);
            }
        }

        IXLeafRecordManager();
    protected:
        unsigned numRIDs;
        std::vector<RID> RIDList;
    };

    class IXIndexRecordManager:public IXRecordManager{
    public:
        IXIndexRecordManager(const Attribute &indexAttr, void* data)
                : IXRecordManager(indexAttr,data){
            this->recordLen = 0;
            auto* dataC = static_cast<unsigned char*>(data);
            if(indexAttr.type == 2){
                dataC += UNSIGNED_SZ;
                this->recordLen += UNSIGNED_SZ;
            }
            this->recordLen += this->len;
            memcpy(&this->rightChild,dataC + this->len,UNSIGNED_SZ);
            this->recordLen += UNSIGNED_SZ;
        }
        PageNum getRightChild(){
            return this->rightChild;
        }
    protected:
        PageNum rightChild;
    };

    class IXPageManager{
    public:

        IXPageManager(void* data){
            this->pageData = data;
            this->isLeaf = checkLeaf();
            this->numEntries = calcNumEntries();
            this->freeBytes = calcFreeBytes();
            this->rightSibling = calcRightSibling();
        }

        bool checkLeaf(){
            auto* dataC = static_cast<unsigned char*>(this->pageData);
            char res;
            memcpy(&res,dataC + PAGE_SIZE - 1, 1);
            return res == 1;
        }
        unsigned calcNumEntries(){
            auto* dataC = static_cast<unsigned char*>(this->pageData);
            unsigned short res;
            memcpy(&res,dataC + PAGE_SIZE - 3, 2);
            return res;
        }
        unsigned calcFreeBytes(){
            auto* dataC = static_cast<unsigned char*>(this->pageData);
            unsigned short res;
            memcpy(&res,dataC + PAGE_SIZE - 5, 2);
            return res;
        }
        PageNum calcRightSibling(){
            auto* dataC = static_cast<unsigned char*>(this->pageData);
            PageNum rightSibling;
            memcpy(&rightSibling,dataC + PAGE_SIZE - 9, UNSIGNED_SZ);
            return rightSibling;
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

        void setRightSibling(PageNum rightSib){
            auto* pageDataC = static_cast<char *>(this->pageData);
            memcpy(pageDataC + PAGE_SIZE - 9, &rightSib, UNSIGNED_SZ);
            this->rightSibling = rightSib;
        }

        void* getPageData(){
            return this->pageData;
        }

        unsigned getMetadataLen(){
            return 1 + (UNSIGNED_SZ * 2);
        }
        unsigned getTotalIndexEntriesLen(){
            return PAGE_SIZE - this->freeBytes - this->getMetadataLen();
        }

        void updateNumEntries(int entriesChange){
            this->numEntries += entriesChange;
            auto* dataC = static_cast<char*>(this->pageData);
            memcpy(dataC + PAGE_SIZE - 3, &this->numEntries, 2);
        }

        void updateFreeBytes(int freeBytesChange){
            this->freeBytes += freeBytesChange;
            auto* dataC = static_cast<char*>(this->pageData);
            memcpy(dataC + PAGE_SIZE - 5, &this->freeBytes, 2);
        }

        unsigned getInsertOff(const Attribute &indexAttr, int key){
            unsigned currOff = 0;
            auto* pageC = static_cast<char*>(this->pageData);
            for(int i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    int curKey;
                    memcpy(&curKey, leafRec.getKey(), UNSIGNED_SZ);
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    int curKey;
                    memcpy(&curKey, indexRec.getKey(), UNSIGNED_SZ);
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += indexRec.getRecordLen();
                }
            }
            return currOff;
        }

        unsigned getInsertOff(const Attribute &indexAttr, float key){
            unsigned currOff = 0;
            auto* pageC = static_cast<char*>(this->pageData);
            for(int i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    float curKey;
                    memcpy(&curKey, leafRec.getKey(), UNSIGNED_SZ);
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    float curKey;
                    memcpy(&curKey, indexRec.getKey(), UNSIGNED_SZ);
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += indexRec.getRecordLen();
                }
            }
            return currOff;
        }


        unsigned getInsertOff(const Attribute &indexAttr, const std::string &key){
            unsigned currOff = 0;
            auto* pageC = static_cast<char*>(this->pageData);
            for(int i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    std::string curKey(reinterpret_cast<char const *>(leafRec.getKey()), leafRec.getKeyLen());
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    std::string curKey(reinterpret_cast<char const *>(indexRec.getKey()), indexRec.getKeyLen());
                    if(key <= curKey){
                        return currOff;
                    }
                    currOff += indexRec.getRecordLen();
                }
            }
            return currOff;
        }

        void getKeys(const Attribute &indexAttr, std::vector<IntKey> &keyList){
            unsigned currOff = 0;
            auto* pageC = static_cast<unsigned char*>(this->pageData);
            for(unsigned i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    IntKey intKey;
                    memcpy(&intKey.key,leafRec.getKey(),UNSIGNED_SZ);
                    intKey.offset = currOff;
                    keyList.push_back(intKey);
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    IntKey ik;
                    memcpy(&ik.key,indexRec.getKey(),UNSIGNED_SZ);
                    ik.offset = currOff;
                    keyList.push_back(ik);
                    currOff += indexRec.getRecordLen();
                }
            }
        }

        void getKeys(const Attribute &indexAttr, std::vector<FloatKey> &keyList){
            unsigned currOff = 0;
            auto* pageC = static_cast<unsigned char*>(this->pageData);
            for(unsigned i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    FloatKey floatKey;
                    memcpy(&floatKey.key,leafRec.getKey(),UNSIGNED_SZ);
                    floatKey.offset = currOff;
                    keyList.push_back(floatKey);
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    FloatKey floatKey;
                    memcpy(&floatKey.key,indexRec.getKey(),UNSIGNED_SZ);
                    floatKey.offset = currOff;
                    keyList.push_back(floatKey);
                    currOff += indexRec.getRecordLen();
                }
            }
        }

        void getKeys(const Attribute &indexAttr, std::vector<SKey> &keyList){
            unsigned currOff = 0;
            auto* pageC = static_cast<unsigned char*>(this->pageData);
            for(unsigned i = 0; i < this->numEntries; i++){
                if(this->isLeaf){
                    IXLeafRecordManager leafRec(indexAttr,pageC + currOff);
                    SKey sKey;
                    memcpy(&sKey.key,leafRec.getKey(),leafRec.getKeyLen());
                    sKey.offset = currOff;
                    keyList.push_back(sKey);
                    currOff += leafRec.getRecordLen();
                }else{
                    IXIndexRecordManager indexRec(indexAttr,pageC + currOff);
                    SKey sKey;
                    memcpy(&sKey.key,indexRec.getKey(),indexRec.getKeyLen());
                    sKey.offset = currOff;
                    keyList.push_back(sKey);
                    currOff += indexRec.getRecordLen();
                }
            }
        }

    protected:
        bool isLeaf;
        unsigned numEntries;
        unsigned freeBytes;
        PageNum rightSibling;
        void* pageData;
    };

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
        IXFileHandle ixFileHandle;
        Attribute attribute;
        const void* lowKey;
        const void* highKey;
        bool lowKeyInc;
        bool highKeyInc;
        PageNum currpage;
        unsigned currOff; //start of next record
        unsigned currEntryNum; //index entry
        unsigned numRIDs; //rids corr to one key
        std::vector<RID> RIDList; // list of relevant rids
        IXLeafRecordManager curLeafRec;
    };
}// namespace PeterDB
#endif // _ix_h_
