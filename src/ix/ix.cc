 #include "src/include/ix.h"
using namespace std;
namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    PagedFileManager &pfm = PagedFileManager::instance();

    void initPage(void * page, bool isLeaf){
        unsigned freeBytes = PAGE_SIZE - (1 + (UNSIGNED_SZ * 2)); // leaf | free | entries | right child
        unsigned rightChild = 0;
        unsigned short isLeafVal = 0;
        if(isLeaf){
            isLeafVal = 1;
        }
        auto* pageC = static_cast<unsigned char*>(page);
        memcpy(pageC + PAGE_SIZE - 1, &isLeafVal, 1);
        memcpy(pageC + PAGE_SIZE - (1 + UNSIGNED_SZ), &freeBytes, UNSIGNED_SZ / 2);
        memcpy(pageC + PAGE_SIZE - (1 + (UNSIGNED_SZ * 2)), &rightChild, UNSIGNED_SZ);
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return pfm.createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC initIndexFile(IXFileHandle &ixFileHandle){
        ixFileHandle.init = true;
        if(ixFileHandle.fileHandle.numPages == 0){
            //need to initialize index file
            void* dummy = calloc(1,PAGE_SIZE); //dummy pointer page
            unsigned rootPageNum = 1; //since dummy page is page 0
            memcpy(dummy,&rootPageNum,UNSIGNED_SZ);
            RC rc1 = ixFileHandle.fileHandle.appendPage(dummy);
            free(dummy);

            auto* rootPage = static_cast<char*>(calloc(1,PAGE_SIZE));
            initPage(rootPage, false);
            unsigned leftChildPage = rootPageNum + 1;
            memcpy(rootPage + PAGE_SIZE - 9,&leftChildPage,UNSIGNED_SZ); //initialize left child of root
            RC rc2 = ixFileHandle.fileHandle.appendPage(rootPage);
            free(rootPage);

            void* leafPage = calloc(1,PAGE_SIZE); //initialize left child page of root
            initPage(leafPage,true);
            RC rc3 = ixFileHandle.fileHandle.appendPage(leafPage);
            free(leafPage);

            if(rc1 == -1 || rc2 == -1 || rc3 == -1)
                return -1;
        }
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if(ixFileHandle.fileHandle.fileP != nullptr)
            return -1;
        RC rc = pfm.openFile(fileName,ixFileHandle.fileHandle);
        initIndexFile(ixFileHandle);
        ixFileHandle.root = 1;
        return rc;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return pfm.closeFile(ixFileHandle.fileHandle);
    }

    unsigned intBinSrch(unsigned key, vector<IntKey> vec){
        unsigned low = 0, high = vec.size() - 2;
        while(low <= high){
            unsigned mid = (low + high) / 2;
            if(vec[mid].key < key){
                low = mid + 1;
            }else if(vec[mid].key > key){
                high = mid - 1;
            }else{
                return mid;
            }
        }
        return high + 1;
    }

    unsigned floatBinSrch(float key, vector<FloatKey> vec){
        unsigned low = 0, high = vec.size() - 2;
        while(low <= high){
            unsigned mid = (low + high) / 2;
            if(vec[mid].key < key){
                low = mid + 1;
            }else if(vec[mid].key > key){
                high = mid - 1;
            }else{
                return mid;
            }
        }
        return high + 1;
    }

    unsigned strBinSrch(const char* key, vector<SKey> vec){
        unsigned low = 0, high = vec.size() - 2;
        while(low <= high){
            unsigned mid = (low + high) / 2;
            unsigned val = strcmp(key,vec[mid].key);
            if(val > 0){
                low = mid + 1;
            }else if(val == 0){
                return mid;
            }else{
                high = mid - 1;
            }
        }
        return high + 1;
    }

    unsigned* getInsertLoc(IXPageManager page, void* pageData, const Attribute &attribute, const void* key){
        unsigned insertOff = 0;
        unsigned shiftLen = 0;
        switch(attribute.type){
            case 0:
            {
                unsigned intKeyVal;
                memcpy(&intKeyVal,key,UNSIGNED_SZ);
                vector<IntKey> vec;
                vec.reserve(page.getNumEntries());
                page.getKeys(attribute,vec);
                insertOff = vec[intBinSrch(intKeyVal,vec)].offset;
                shiftLen = vec[vec.size() - 1].offset - insertOff;
                break;
            }
            case 1:
            {
                float floatKeyVal;
                memcpy(&floatKeyVal, key, UNSIGNED_SZ);
                vector<FloatKey> vec;
                vec.reserve(page.getNumEntries());
                page.getKeys(attribute,vec);
                insertOff = vec[floatBinSrch(floatKeyVal,vec)].offset;
                shiftLen = vec[vec.size() - 1].offset - insertOff;
                break;
            }
            case 2:
            {
                unsigned len;
                memcpy(&len, key, UNSIGNED_SZ);
                auto *keyC = static_cast<const char *>(key);
                std::string sKeyVal(reinterpret_cast<char const *>(keyC + UNSIGNED_SZ), len);
                vector<SKey> vec;
                vec.reserve(page.getNumEntries());
                page.getKeys(attribute,vec);
                insertOff = vec[strBinSrch(keyC,vec)].offset;
                shiftLen = vec[vec.size() - 1].offset - insertOff;
                break;
            }
        }
        unsigned arr[] = {insertOff,shiftLen};
        return arr;
    }

    unsigned createLeafRec(void* data, const Attribute &attribute, const void *key, const RID &rid){
        unsigned recLen = 0;
        unsigned keyLen = UNSIGNED_SZ;
        auto* dataC = static_cast<unsigned char*>(data);
        auto* keyC = static_cast<const char*>(key);
        if(attribute.type == 2){
            memcpy(dataC,keyC,UNSIGNED_SZ);
            memcpy(&keyLen,keyC,UNSIGNED_SZ);
            recLen += UNSIGNED_SZ;
            dataC += UNSIGNED_SZ;
            keyC += UNSIGNED_SZ;
        }
        memcpy(dataC,keyC,keyLen);
        recLen += keyLen;
        dataC += keyLen;

        recLen += UNSIGNED_SZ; //number of RIDs
        unsigned numRIDs = 1;
        memcpy(dataC,&numRIDs,UNSIGNED_SZ);
        dataC += UNSIGNED_SZ;

        recLen += UNSIGNED_SZ + (UNSIGNED_SZ / 2); //size of RID = page num + slot num = 4 + 2
        memcpy(dataC, &rid.pageNum,UNSIGNED_SZ);
        memcpy(dataC + UNSIGNED_SZ,&rid.slotNum,UNSIGNED_SZ / 2);

        return recLen;
    }

    bool keyExists(void* page, const Attribute &attribute, unsigned insertOff, const void* key){
        auto* pageC = static_cast<unsigned char*>(page);
        switch(attribute.type){
            case 0: unsigned newIKey;
                memcpy(&newIKey, key, UNSIGNED_SZ);
                unsigned oldIKey;
                memcpy(&oldIKey,pageC + insertOff,UNSIGNED_SZ);
                if(oldIKey == newIKey)
                    return true;
                else
                    return false;
            case 1: float newFKey;
                memcpy(&newFKey, key, UNSIGNED_SZ);
                float oldFKey;
                memcpy(&oldFKey,pageC + insertOff,UNSIGNED_SZ);
                if(oldFKey == newFKey)
                    return true;
                else
                    return false;
            case 2: auto* keyC = static_cast<const char*>(key);
                unsigned oldlen;
                memcpy(&oldlen,pageC + insertOff,UNSIGNED_SZ);
                unsigned newLen;
                memcpy(&newLen, key, UNSIGNED_SZ);
                std::string oldVal( reinterpret_cast<char const*>(pageC + insertOff + UNSIGNED_SZ), oldlen ) ;
                std::string newVal( reinterpret_cast<char const*>(keyC + UNSIGNED_SZ), newLen ) ;
                if(strcmp(oldVal.c_str(),newVal.c_str()) == 0)
                    return true;
                else
                    return false;
        }
    }

    bool isGreaterKey(const Attribute &attribute, IXIndexRecordManager indexRec, const void* key){
        switch(attribute.type){
            case 0: unsigned iKey;
                memcpy(&iKey,indexRec.getKey(),UNSIGNED_SZ);
                unsigned ikeyToInsert;
                memcpy(&ikeyToInsert,key,UNSIGNED_SZ);
                if(iKey > ikeyToInsert)
                    return true;
                else
                    return false;
            case 1: unsigned fKey;
                memcpy(&fKey,indexRec.getKey(),UNSIGNED_SZ);
                unsigned fkeyToInsert;
                memcpy(&fkeyToInsert,key,UNSIGNED_SZ);
                if(fKey > fkeyToInsert)
                    return true;
                else
                    return false;
            case 2:
                auto* sKey = static_cast<char *>(indexRec.getKey());
                unsigned keyLen;
                memcpy(&keyLen,sKey,UNSIGNED_SZ);
                std::string sKeyVal(reinterpret_cast<char const *>(sKey + UNSIGNED_SZ), keyLen);

                auto* keyC = static_cast<const char *>(key);
                unsigned newKeylen;
                memcpy(&newKeylen,keyC,UNSIGNED_SZ);
                std::string sKeyToInsert(reinterpret_cast<char const *>(keyC + UNSIGNED_SZ), newKeylen);
                if(strcmp(sKeyVal.c_str(),sKeyToInsert.c_str()) > 0)
                    return true;
                else
                    return false;
        }
    }

    unsigned createIndexRec(void* data, const Attribute &attribute, void *key, PageNum &rightChild){
        unsigned len = UNSIGNED_SZ;
        unsigned size = len; //key for int, real; len for varchar
        if(attribute.type == 2){
            memcpy(&len, key, UNSIGNED_SZ);
            size += len; //key for varchar
        }
        auto* dataC = static_cast<char *>(data);
        memcpy(dataC,key,size);
        size += UNSIGNED_SZ; //right child
        memcpy(dataC + size, &rightChild,UNSIGNED_SZ);
        return size;
    }

    void splitPage(IXPageManager oldPage, IXPageManager newPage, const Attribute &indexAttr, void* recordData, const void* key, unsigned newRecSize, void* splitKey){
        auto* oldPageC = static_cast<char *>(oldPage.getPageData());
        auto* newPageC = static_cast<char *>(newPage.getPageData());
        switch(indexAttr.type) {
            case 0: {
                vector<IntKey> vec;
                oldPage.getKeys(indexAttr, vec);
                unsigned keyVal;
                memcpy(&keyVal, key, UNSIGNED_SZ);
                unsigned ind = intBinSrch(keyVal, vec);
                unsigned mid = (oldPage.getNumEntries() + 1) / 2; // + 1 for new entry to find mid

                //move the 2nd half of data to new page
                unsigned bytesToShift = vec[vec.size() - 1].offset - vec[mid].offset;
                memcpy(newPageC,oldPageC + vec[mid].offset, bytesToShift);
                unsigned entriesMoved = oldPage.getNumEntries() - mid + 1;
                oldPage.updateFreeBytes(bytesToShift);
                oldPage.updateNumEntries(entriesMoved * -1);

                newPage.updateNumEntries(entriesMoved);
                newPage.updateFreeBytes(bytesToShift * -1);

                if(ind < mid){
                    //add new key to the old page
                    unsigned shiftBytesNewRec = vec[mid].offset - vec[ind].offset; //bytes to shift for new record
                    memmove(oldPageC + vec[ind].offset + newRecSize, oldPageC + vec[ind].offset, shiftBytesNewRec);
                    memmove(oldPageC + vec[ind].offset,recordData,newRecSize);
                } else{
                    //right page
                    unsigned offsetChange = vec[mid].offset; //offset will change by offset of mid record; it will reduce by this value
                    unsigned shiftBytes = (vec[vec.size() - 1].offset - offsetChange) - (vec[ind].offset - offsetChange);
                    memmove(newPageC + vec[ind].offset - offsetChange + newRecSize, oldPageC + vec[ind].offset - offsetChange, shiftBytes);
                    memmove(newPageC + vec[ind].offset - offsetChange,recordData,newRecSize);
                }
                splitKey = calloc(1,UNSIGNED_SZ);
                memcpy(splitKey, &vec[ind].key, UNSIGNED_SZ);
                break;
            }
            case 1: break;
            case 2: break;
        }
    }

    RC insertHelper(PageNum node, IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, void* newKey, PageNum &newPageEntry){
        void* data = malloc(PAGE_SIZE);
        ixFileHandle.fileHandle.readPage(node, data);
        IXPageManager page(data);
        auto *dataC = static_cast<char*>(data);

        if(page.getIsLeaf()){
            unsigned* temp = getInsertLoc(page,data,attribute,key);  //find insert offset
            unsigned insertOff = temp[0];
            unsigned shiftLen = temp[1];
            unsigned size;
            void* dataToInsert = calloc(1,PAGE_SIZE);
            if(shiftLen == 0 || !keyExists(data,attribute,insertOff,key)){
                size = createLeafRec(dataToInsert,attribute,key,rid); //key doesn't exist
            }else{
                size = UNSIGNED_SZ + (UNSIGNED_SZ / 2);//need space only for 1 RID i.e. 6 bytes
            }

            if(page.getFreeBytes() >= size){
                //enough space on page -> shift bytes to the right
                memmove(dataC + insertOff + size, dataC + insertOff, shiftLen);
                memmove(dataC + insertOff,dataToInsert,size);
                if(size == 6){
                     unsigned bytes = UNSIGNED_SZ; //only RIDs updated; so update numRID in current record
                     if(attribute.type == 2){
                         unsigned len;
                         memcpy(&len,dataC + insertOff,UNSIGNED_SZ);
                         bytes += len;
                     }
                     unsigned currNumRids;
                     memcpy(&currNumRids,dataC + insertOff + bytes, UNSIGNED_SZ);
                     currNumRids++;
                     memcpy(dataC + insertOff + bytes, &currNumRids, UNSIGNED_SZ);
                }else{
                    page.updateNumEntries(1);
                }
                page.updateFreeBytes(size * -1);
                newPageEntry = 0;
                newKey = nullptr;
            }else{
                void* newPageData = calloc(1,PAGE_SIZE);
                initPage(newPageData,true);
                IXPageManager newPage(newPageData);
                splitPage(page,newPage,attribute,dataToInsert,key,size, newKey);
                ixFileHandle.fileHandle.appendPage(newPageData);
                newPageEntry = ixFileHandle.fileHandle.getNumberOfPages() - 1;
            }

        }else{
            //find subtree to navigate to; iterate linearly through keys on page
            unsigned numEntries = page.getNumEntries();
            unsigned curr = 0;
            IXIndexRecordManager indexRec(attribute,dataC + curr);
            PageNum nextPage = page.getRightSibling(); //left child of the index node;
            for (unsigned i = 0; i < numEntries; i++) {
                if (isGreaterKey(attribute, indexRec, key)) {
                    break;
                }
                curr += indexRec.getRecordLen();
                nextPage = indexRec.getRightChild();
                indexRec = IXIndexRecordManager(attribute, dataC + curr);
            }
            insertHelper(nextPage, ixFileHandle, attribute, key, rid, newKey, newPageEntry);
            if(newPageEntry == 0)
                return 0;
            else{
                void* indexData = malloc(PAGE_SIZE / 2);
                unsigned newIndRecSize = createIndexRec(indexData,attribute,newKey,newPageEntry);
                if(page.getFreeBytes() >= newIndRecSize){
                    unsigned shiftLen = page.getTotalIndexEntriesLen() - curr;
                    memmove(dataC + curr + newIndRecSize, dataC + curr, shiftLen);
                    memmove(dataC + curr,indexData,newIndRecSize);
                    page.updateNumEntries(1);
                    page.updateFreeBytes(newIndRecSize * -1);
                    newPageEntry = 0;
                    newKey = nullptr;
                    return 0;
                }else{
                    //split ;((( else split 1/2 keys and move to new page
                    void* newPageData = calloc(1, PAGE_SIZE);
                    initPage(newPageData,false);
                    IXPageManager newPage(newPageData);
                    splitPage(page,newPage,attribute,indexData,key,newIndRecSize,newKey);
                    ixFileHandle.fileHandle.appendPage(newPageData);
                    newPageEntry = ixFileHandle.fileHandle.getNumberOfPages() - 1;
                }
            }
        }
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        void* newKey = nullptr;
        PageNum pgnum = 0;
        insertHelper(ixFileHandle.root,ixFileHandle,attribute,key,rid, newKey, pgnum);
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return this->fileHandle.collectCounterValues(readPageCount,writePageCount,appendPageCount);
    }

} // namespace PeterDB