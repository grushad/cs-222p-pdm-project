#include "src/include/rbfm.h"
#include <cmath>
#include <cstring>
#include <iostream>
#include <algorithm>

using namespace std;
namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    PagedFileManager &pfm = PagedFileManager::instance();

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
       return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pfm.openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pfm.closeFile(fileHandle);
    }

    bool checkPageFreeSpace(unsigned recSize, unsigned pageNum, FileHandle &fileHandle){
        auto *page = static_cast<unsigned *>(::calloc(1,PAGE_SIZE));
        if(page == nullptr)
            return  -1;
        fileHandle.readPage(pageNum, page);
        unsigned freeBytes = page[(PAGE_SIZE / UNSIGNED_SZ) - 1]; //free bytes is the last 4 bytes
        free(page);
        recSize += (UNSIGNED_SZ * 2); //add 8 bytes for slot directory to check for avl space
        if(freeBytes >= recSize)
            return true;
        return false;
    }

    unsigned findFreePage(unsigned recSize, FileHandle &fileHandle){
        unsigned numPages = fileHandle.numPages;
        if(numPages == 0)
            return numPages;
        if(checkPageFreeSpace(recSize, numPages - 1,fileHandle)){
            return numPages - 1;
        }
        for(int i = 0; i < numPages - 1; i++){
            if(checkPageFreeSpace(recSize, i,fileHandle))
                return i;
        }
        return numPages;
    }

    void getNullBits(const void * data, unsigned numFields, vector<bool> &nullvec){
        auto * dataC = static_cast<const unsigned char*>(data);
        unsigned numNullBytes = ceil((double)numFields / 8);
        unsigned ind = 0;
        for(unsigned i = 0; i < numNullBytes; i++){
            unsigned char c = dataC[i];
            for(unsigned j = 0; j < 8; j++){
                unsigned power = 8 - j - 1;
                if((c & (1<<power)) != 0){
                    nullvec.push_back(true);
                }else{
                    nullvec.push_back(false);
                }
                ind++;
            }
        }
    }

    unsigned createRecord(const std::vector<Attribute> &recordDescriptor, const void *data, void * record){
        unsigned numFields = recordDescriptor.size();
        vector<bool> nullvec;
        getNullBits(data, numFields,nullvec);
        auto * recData = static_cast<unsigned char*>(record);

        //adding tombstone and rid at the start of the record for update queries
        recData += TMBSTN_SZ + RID_SZ;

        ::memcpy(recData, &numFields, UNSIGNED_SZ);
        recData += UNSIGNED_SZ;

        unsigned numNullBytes = ceil((double)numFields / 8);
        ::memcpy(recData, data, numNullBytes);
        recData += numNullBytes;

        unsigned offset = TMBSTN_SZ + RID_SZ + UNSIGNED_SZ + numNullBytes + (numFields * UNSIGNED_SZ); //tombstone size  + rid size + numFields || null indicator bits || offset for each field
        unsigned recSize = offset;

        auto * diskData = static_cast<const unsigned char *>(data);
        diskData += numNullBytes;

        for(int i = 0; i < numFields; i++){
            Attribute a = recordDescriptor[i];
            if(!nullvec[i]){
                unsigned lenV = a.length;
                if(a.type == 2){
                    ::memcpy(&lenV, diskData, UNSIGNED_SZ);
                    diskData += UNSIGNED_SZ;
                }
                offset += lenV;
                diskData += lenV;
                ::memcpy(recData, &offset, UNSIGNED_SZ);
            }else{
                signed nullPt = -1;
                ::memcpy(recData, &nullPt,UNSIGNED_SZ);
            }
            recData += UNSIGNED_SZ;
        }
        diskData = static_cast<const unsigned char *>(data);
        diskData += numNullBytes;

        for(int i = 0; i < numFields; i++){
            Attribute a = recordDescriptor[i];
            if(!nullvec[i]){
                unsigned len = a.length;
                if(a.type == 2){
                    ::memcpy(&len,diskData,UNSIGNED_SZ);
                    diskData += UNSIGNED_SZ;
                }
                ::memcpy(recData, diskData, len);
                recSize += len;
                recData += len;
                diskData += len;
            }
        }
        return recSize;
    }

    unsigned writeRecord(void * page, unsigned recSize, void * recData){
        auto * pageC = static_cast<unsigned char *>(page);
        //grab the last 2 ints i.e. 8 bytes
        pageC += PAGE_SIZE - (UNSIGNED_SZ * 2);
        unsigned freeBytes = 0;
        unsigned numRecords = 0;

        ::memcpy(&numRecords, pageC,UNSIGNED_SZ);
        pageC += UNSIGNED_SZ;
        ::memcpy(&freeBytes,pageC,UNSIGNED_SZ);
        pageC -= UNSIGNED_SZ;

        unsigned slotNum = numRecords + 1;
        //finding the next available slot to fill as we can't leave holes
        for(unsigned i = 1; i <= numRecords; i++){
            pageC -= (2 * UNSIGNED_SZ);
            unsigned curLen;
            ::memcpy(&curLen,pageC,UNSIGNED_SZ);
            if(curLen == 0){
                //empty slot found
                slotNum = i;
                break;
            }
        }
        if(slotNum == numRecords + 1){
            //no empty slots in b/w
            pageC += (2 * UNSIGNED_SZ * (slotNum - 1));
        }else{
            pageC += (2 * UNSIGNED_SZ * slotNum);
        }


        unsigned offset = 0;
        if(numRecords != 0){
            unsigned maxRecOffset = 0;
            unsigned maxRecLen = 0;
            for(unsigned i = 0; i < numRecords; i++){
                pageC -= UNSIGNED_SZ;
                ::memcpy(&maxRecOffset, pageC, UNSIGNED_SZ);
                pageC -= UNSIGNED_SZ;
                ::memcpy(&maxRecLen, pageC, UNSIGNED_SZ);
                if(offset < (maxRecLen + maxRecOffset))
                    offset = maxRecOffset + maxRecLen;
            }
        }
        pageC += (UNSIGNED_SZ * 2 * numRecords);
        pageC -= (UNSIGNED_SZ * 2 * slotNum); //skipping the slots to fill the next slot for new record

        ::memcpy(pageC, &recSize, UNSIGNED_SZ); //updating length in slot directory
        pageC += UNSIGNED_SZ;
        ::memcpy(pageC, &offset, UNSIGNED_SZ); //updating offset in the slot directory
        pageC -= UNSIGNED_SZ;

        if(freeBytes == 0){
            freeBytes = PAGE_SIZE - (UNSIGNED_SZ * 2);
        }
        freeBytes -= recSize;
        freeBytes -= (UNSIGNED_SZ * 2); //new slot size

        if(slotNum == numRecords + 1) {
            numRecords++;
        }

        pageC += (UNSIGNED_SZ * 2 * slotNum);
        ::memcpy(pageC, &numRecords, UNSIGNED_SZ); //updating metadata, i.e. num of records on page
        pageC += UNSIGNED_SZ;
        ::memcpy(pageC, &freeBytes, UNSIGNED_SZ); //updating num of free bytes on the page.

        pageC += UNSIGNED_SZ - PAGE_SIZE;
        pageC += offset;
        ::memcpy(pageC, recData, recSize);

        return slotNum;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if(fileHandle.fileP == nullptr)
            return -1;

        void * recordData = ::calloc(1, PAGE_SIZE);
        unsigned recSize = createRecord(recordDescriptor, data, recordData);
        unsigned pageNumToWrite = findFreePage(recSize,fileHandle);
        void * pageData = ::calloc(1,PAGE_SIZE);

        if(pageNumToWrite < fileHandle.numPages){
            fileHandle.readPage(pageNumToWrite, pageData);
        }
        unsigned slotNum = writeRecord(pageData, recSize, recordData);
        if(pageNumToWrite == fileHandle.numPages){
            fileHandle.appendPage(pageData);
        }else{
            fileHandle.writePage(pageNumToWrite, pageData);
        }
        rid.pageNum = pageNumToWrite;
        rid.slotNum = slotNum;
        free(recordData);
        free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        if(fileHandle.fileP == nullptr)
            return -1;

        unsigned pageNum = rid.pageNum;
        const unsigned short slotNum = rid.slotNum;

        auto *pageContent = static_cast<unsigned char*>(calloc(1,PAGE_SIZE));
        if(pageContent == nullptr)
            return -1;
        if(fileHandle.readPage(pageNum, pageContent) == -1)
            return -1;

        pageContent += PAGE_SIZE;
        pageContent -= (UNSIGNED_SZ * 2);
        unsigned numRec;
        ::memcpy(&numRec,pageContent,UNSIGNED_SZ);
        if(numRec < slotNum)
            return -2;

        pageContent -= (slotNum * 2 * UNSIGNED_SZ);
        unsigned len = 0;
        ::memcpy(&len, pageContent, UNSIGNED_SZ);
        //check if trying to read a deleted record
        if(len == 0)
            return -1;

        unsigned offset = 0;
        pageContent += UNSIGNED_SZ;
        ::memcpy(&offset, pageContent, UNSIGNED_SZ);

        pageContent -= UNSIGNED_SZ;
        pageContent += (slotNum * 2 * UNSIGNED_SZ);
        pageContent += (UNSIGNED_SZ * 2);
        pageContent -= PAGE_SIZE; //start of the page

        pageContent += offset; //reached start of record
        //check if it is a tombstone record
        unsigned isTombstone = 0;
        ::memcpy(&isTombstone,pageContent,TMBSTN_SZ);
        if(isTombstone == 1){
            //read the rids and call the read record func recursively
            RID rid0;
            ::memcpy(&rid0.pageNum,pageContent + TMBSTN_SZ,UNSIGNED_SZ);
            ::memcpy(&rid0.slotNum,pageContent + TMBSTN_SZ + UNSIGNED_SZ,UNSIGNED_SZ);
            readRecord(fileHandle,recordDescriptor,rid0,data);
        }else {
            pageContent += TMBSTN_SZ + RID_SZ;
            pageContent += UNSIGNED_SZ; //skip number of fields i.e. 4 bytes

            //create space in memory to store data in API format
            auto *diskD = static_cast<unsigned char *>(calloc(1, PAGE_SIZE));
            unsigned recSz = 0;
            unsigned numFields = recordDescriptor.size();
            unsigned nullInd = ceil((double) numFields / 8);

            memcpy(diskD, pageContent, nullInd); //copy null indicator bits to disk format

            vector<bool> nullvec;
            getNullBits(diskD, numFields, nullvec);
            pageContent += nullInd; //skip the null indicator bits
            diskD += nullInd;
            recSz += nullInd;

            //for each field copy its data and length
            unsigned startRecData = TMBSTN_SZ + RID_SZ + UNSIGNED_SZ + nullInd + (numFields * UNSIGNED_SZ);
            unsigned recLen = 0;
            unsigned offsetLen = numFields * UNSIGNED_SZ;
            for (int i = 0; i < numFields; i++) {
                if (!nullvec[i]) {
                    Attribute a = recordDescriptor[i];
                    unsigned offsetRec;
                    ::memcpy(&offsetRec, pageContent, UNSIGNED_SZ);
                    unsigned len = offsetRec - startRecData;
                    if (a.type == 2) {
                        //type varchar so append length
                        memcpy(diskD, &len, UNSIGNED_SZ);
                        recSz += UNSIGNED_SZ;
                        diskD += UNSIGNED_SZ;
                    }
                    ::memcpy(diskD, pageContent + offsetLen + recLen, len);
                    recSz += len;
                    diskD += len;
                    startRecData += len;
                    recLen += len;
                }
                pageContent += UNSIGNED_SZ; //moves ahead by size of offset
                offsetLen -= UNSIGNED_SZ;
            }
            //at last copy the record data in the API format to data
            memcpy(data, diskD - recSz, recSz);
        }
//        free(pageContent);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        //find page and slot number and read the page in memory
        unsigned pageNum = rid.pageNum;
        const unsigned short slotNum = rid.slotNum;
        auto * pageData = static_cast<unsigned char *>(::calloc(1,PAGE_SIZE));
        if(pageData == nullptr)
            return -1;
        if(fileHandle.readPage(pageNum, pageData) == -1)
            return -1;

        //find the record length and offset from slot directory
        pageData += PAGE_SIZE - UNSIGNED_SZ;
        unsigned freeBytes;
        ::memcpy(&freeBytes,pageData,UNSIGNED_SZ);
        pageData -= UNSIGNED_SZ;
        unsigned numRec;
        ::memcpy(&numRec,pageData,UNSIGNED_SZ);
        pageData -= (2 * slotNum * UNSIGNED_SZ);
        unsigned currLen;
        unsigned currOffset;
        ::memcpy(&currLen, pageData, UNSIGNED_SZ);
        pageData += UNSIGNED_SZ;
        ::memcpy(&currOffset, pageData, UNSIGNED_SZ);

        //set the current records length to 0 to indicate a deleted record.
        pageData -= UNSIGNED_SZ;
        unsigned newLen = 0;
        ::memcpy(pageData,&newLen, UNSIGNED_SZ);

        //check if record already deleted
        if(currLen == 0)
            return -1;

        //find the length & offset of last record
        pageData -= ((numRec - slotNum)* 2 * UNSIGNED_SZ);
        unsigned lastLen = currLen;
        unsigned lastOffset = currOffset;
        for(unsigned i = 0; i < numRec; i++){
            unsigned len;
            unsigned off;
            ::memcpy(&len, pageData, UNSIGNED_SZ);
            pageData += UNSIGNED_SZ;
            ::memcpy(&off,pageData,UNSIGNED_SZ);
            pageData += UNSIGNED_SZ;
            if(lastOffset < off){
                lastOffset = off;
                lastLen = len;
            }
        }
        pageData += (UNSIGNED_SZ * 2);
        pageData -= PAGE_SIZE; //at the start of the page

        //check if it is a tombstone record
        unsigned tombstone;

        ::memcpy(&tombstone,pageData + currOffset, sizeof(tombstone));
        if(tombstone == 1){
            //save the next rid and delete that recursively until actual data found;
            RID rid0;
            ::memcpy(&rid0.pageNum,pageData + currOffset + TMBSTN_SZ,UNSIGNED_SZ);
            ::memcpy(&rid0.slotNum,pageData + currOffset + TMBSTN_SZ + UNSIGNED_SZ,UNSIGNED_SZ);
            deleteRecord(fileHandle,recordDescriptor,rid0);
        }

        unsigned bytesToShift = (lastOffset + lastLen) - (currOffset + currLen);
        ::memmove(pageData + currOffset, pageData + currOffset + currLen, bytesToShift);

        //update all values in the slot directory after curr record
        pageData += PAGE_SIZE; //at the end of the page
        pageData -= (UNSIGNED_SZ * 2); //skipping free bytes & num rec
        for(unsigned i = 0; i < numRec; i++){
            pageData -= UNSIGNED_SZ;
            unsigned off;
            ::memcpy(&off,pageData,UNSIGNED_SZ);
            if(off >= (currLen + currOffset)) {
                off -= currLen;
                ::memcpy(pageData, &off, UNSIGNED_SZ);
            }
            pageData -= UNSIGNED_SZ;
        }

        //update metadata i.e.free bytes and number of records
        pageData += (numRec * 2 * UNSIGNED_SZ);
        freeBytes += (currLen + (UNSIGNED_SZ * 2)); //record size  + slot directory used by record
        pageData += UNSIGNED_SZ;
        ::memcpy(pageData,&freeBytes,UNSIGNED_SZ);
        pageData += UNSIGNED_SZ - PAGE_SIZE;
        fileHandle.writePage(pageNum,pageData);
        free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned numFields = recordDescriptor.size();
        unsigned nullBytes = ceil((double)numFields / 8);
        auto * recData = static_cast<const unsigned char *>(data);
        vector<bool> nullvec;
        getNullBits(data, numFields,nullvec);
        recData += nullBytes;
        for(unsigned i = 0; i < numFields; i++){
            Attribute a = recordDescriptor[i];
            std::string name = a.name;
            if(nullvec[i]){
                if(i == numFields -1)
                    out<<name<<": NULL";
                else
                    out<<name<<": NULL,";
            }else{
                if(a.type == 0){
                    //int
                    unsigned val;
                    ::memcpy(&val,recData,UNSIGNED_SZ);
                    recData += UNSIGNED_SZ;
                    if(i == numFields -1)
                        out<<name<<": "<<val;
                    else
                        out<<name<<": "<<val<<", ";
                }else if(a.type == 1){
                    //float
                    float val;
                    ::memcpy(&val, recData, UNSIGNED_SZ);
                    recData += UNSIGNED_SZ;
                    if(i == numFields -1)
                        out<<name<<": "<<val;
                    else
                        out<<name<<": "<<val<<", ";
                }else{
                    //varchar
                    unsigned len;
                    ::memcpy(&len, recData,UNSIGNED_SZ);
                    recData += UNSIGNED_SZ;
                    std::string val( reinterpret_cast<char const*>(recData), len ) ;
                    recData += len;
                    if(i == numFields -1)
                        out<<name<<": "<<val;
                    else
                        out<<name<<": "<<val<<", ";
                }
            }
        }
        out<<endl;
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        //find page and slot number and read the page in memory
        unsigned pageNum = rid.pageNum;
        const unsigned short slotNum = rid.slotNum;
        auto * pageData = static_cast<unsigned char *>(calloc(1,PAGE_SIZE));
        if(pageData == nullptr)
            return -1;
        if(fileHandle.readPage(pageNum, pageData) == -1)
            return -1;

        //find the record length and offset from slot directory
        pageData += PAGE_SIZE - UNSIGNED_SZ;
        unsigned freeBytes;
        ::memcpy(&freeBytes,pageData,UNSIGNED_SZ);
        pageData -= UNSIGNED_SZ;
        unsigned numRec;
        ::memcpy(&numRec,pageData,UNSIGNED_SZ);
        pageData -= (2 * slotNum * UNSIGNED_SZ);
        unsigned currLen;
        unsigned currOffset;
        ::memcpy(&currLen, pageData, UNSIGNED_SZ);
        //check if trying to update a deleted record
        if(currLen == 0)
            return -1;
        ::memcpy(&currOffset, pageData + UNSIGNED_SZ, UNSIGNED_SZ);
        pageData += (2 * slotNum * UNSIGNED_SZ); //skip the slot directory

        //find the length & offset of last record
        pageData -= (numRec * 2 * UNSIGNED_SZ);
        unsigned lastLen = currLen;
        unsigned lastOffset = currOffset;
        for(unsigned i = 0; i < numRec; i++){
            unsigned len;
            unsigned off;
            ::memcpy(&len, pageData, UNSIGNED_SZ);
            pageData += UNSIGNED_SZ;
            ::memcpy(&off,pageData,UNSIGNED_SZ);
            pageData += UNSIGNED_SZ;
            if(lastOffset < off){
                lastOffset = off;
                lastLen = len;
            }
        }
        pageData += (2 * UNSIGNED_SZ); //skip free bytes and num rec
        pageData -= PAGE_SIZE; //at the start of page
        unsigned isTmbstn;
        ::memcpy(&isTmbstn,pageData + currOffset,TMBSTN_SZ);
        if(isTmbstn == 1)
            return 0;

        //create new record
        void *newRecord = ::calloc(1,PAGE_SIZE);
        unsigned newSize = createRecord(recordDescriptor,data,newRecord);

        //check old record size compared to new record size
        if(newSize <= currLen){
            //copy new record
            ::memcpy(pageData + currOffset,newRecord,newSize);
            if(newSize < currLen){
                //shift left
                unsigned bytesToShift = (lastOffset + lastLen) - (currOffset + currLen);
                ::memmove(pageData + currOffset + newSize, pageData + currOffset + currLen, bytesToShift);

                //update offsets of all other records to the left
                pageData += PAGE_SIZE; //at the end of the page
                pageData -= (UNSIGNED_SZ * 2); //skipping free bytes & num rec
                for(unsigned i = 0; i < numRec; i++){
                    pageData -= UNSIGNED_SZ;
                    unsigned off;
                    ::memcpy(&off,pageData,UNSIGNED_SZ);
                    if(off >= (currLen + currOffset)) {
                        off -= (currLen - newSize) ;
                        ::memcpy(pageData, &off, UNSIGNED_SZ);
                    }
                    pageData -= UNSIGNED_SZ;
                    //update length of record in slot directory
                    if(slotNum == (i + 1)){
                        ::memcpy(pageData,&newSize,UNSIGNED_SZ);
                    }
                }
                pageData += (numRec * 2 * UNSIGNED_SZ);
                //update free bytes
                unsigned newFreeBytes = freeBytes + (currLen - newSize);
                ::memcpy(pageData + UNSIGNED_SZ,&newFreeBytes,UNSIGNED_SZ);
                pageData += (2 * UNSIGNED_SZ);
                pageData -= PAGE_SIZE; //at the start of the page
            }
        }else{
            //new record is longer than old record so check if free space is enough or not
            if(freeBytes >= (newSize - currLen)){
                //enough free space so shift last records to right
                unsigned bytesToShift = (lastOffset + lastLen) - (currOffset + currLen);
                ::memmove(pageData + currOffset + newSize, pageData + currOffset + currLen,bytesToShift);
                ::memcpy(pageData + currOffset,newRecord,newSize);//and copy new record

                //update offset for all other records to the right of curr record to right
                pageData += PAGE_SIZE; //at the end of the page
                pageData -= (UNSIGNED_SZ * 2); //skipping free bytes & num rec
                for(unsigned i = 0; i < numRec; i++){
                    pageData -= UNSIGNED_SZ;
                    unsigned off;
                    ::memcpy(&off,pageData,UNSIGNED_SZ);
                    if(off >= (currLen + currOffset)) {
                        off += (newSize - currLen) ;
                        ::memcpy(pageData, &off, UNSIGNED_SZ);
                    }
                    pageData -= UNSIGNED_SZ;
                    //update length of record in slot directory
                    if(slotNum == (i + 1)){
                        ::memcpy(pageData,&newSize,UNSIGNED_SZ);
                    }
                }
                pageData += (numRec * 2 * UNSIGNED_SZ);
                //update free bytes
                unsigned newFreeBytes = freeBytes - (newSize - currLen);
                ::memcpy(pageData + UNSIGNED_SZ,&newFreeBytes,UNSIGNED_SZ);
                pageData += (2 * UNSIGNED_SZ);
                pageData -= PAGE_SIZE; //at the start of the page

            }else{
                //not enough free space to update new record on same page
                //create a tombstone at current spot
                unsigned newPageNum = findFreePage(newSize,fileHandle);
                void * page = ::calloc(1,PAGE_SIZE);
                if(newPageNum < fileHandle.numPages){
                    fileHandle.readPage(newPageNum, page);
                }
                unsigned newSlotNum = writeRecord(page, newSize, newRecord);
                if(newPageNum == fileHandle.numPages){
                    fileHandle.appendPage(page);
                }else{
                    fileHandle.writePage(newPageNum, page);
                }
                short tmbstnVal = 1;
                ::memcpy(pageData + currOffset, &tmbstnVal, TMBSTN_SZ);
                ::memcpy(pageData + currOffset + TMBSTN_SZ, &newPageNum, UNSIGNED_SZ);
                ::memcpy(pageData + currOffset + TMBSTN_SZ + UNSIGNED_SZ, &newSlotNum, UNSIGNED_SZ);
                free(page);
            }
        }
        fileHandle.writePage(pageNum,pageData);
        free(pageData);
        return 0;
    }

    unsigned getAttrNum(const std::string &attrName, const std::vector<Attribute> &recordDescriptor){
        unsigned sz = recordDescriptor.size();
        for(auto i = 0; i < sz; i++){
            if(::strcmp(attrName.c_str(),recordDescriptor[i].name.c_str()) == 0){
                return i;
            }
        }
        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        //find page and slot number and read the page in memory
        unsigned pageNum = rid.pageNum;
        const unsigned short slotNum = rid.slotNum;
        void *temp = ::calloc(1, PAGE_SIZE);
        auto *pageData = static_cast<unsigned char *>(temp);
        if (pageData == nullptr)
            return -1;
        if (fileHandle.readPage(pageNum, pageData) == -1)
            return -1;

        //find the record length and offset from slot directory
        pageData += PAGE_SIZE;
        pageData -= (2 * slotNum * UNSIGNED_SZ + (2 * UNSIGNED_SZ));
        unsigned len;
        unsigned offset;
        ::memcpy(&len, pageData, UNSIGNED_SZ);
        pageData += UNSIGNED_SZ;
        ::memcpy(&offset, pageData, UNSIGNED_SZ);

        //check if trying to read a deleted record
        if (len == 0)
            return -1;

        pageData -= UNSIGNED_SZ;
        pageData += (slotNum * 2 * UNSIGNED_SZ + (UNSIGNED_SZ * 2));
        pageData -= PAGE_SIZE; //start of the page

        pageData += offset; //start of record
        pageData += TMBSTN_SZ + RID_SZ + UNSIGNED_SZ;
        unsigned numFields = recordDescriptor.size();
        unsigned nullInd = ceil((double) numFields / 8);
        vector<bool> nullvec;
        getNullBits(pageData, numFields, nullvec);
        pageData += nullInd;

        unsigned startRecData = TMBSTN_SZ + RID_SZ + UNSIGNED_SZ + nullInd + (numFields * UNSIGNED_SZ);
        unsigned recLen = 0;
        unsigned offsetLen = numFields * UNSIGNED_SZ;

        for(unsigned i = 0; i < numFields; i++){
            if(!nullvec[i]){
                //attribute is not null
                Attribute a = recordDescriptor[i];
                unsigned offsetField;
                ::memcpy(&offsetField,pageData,UNSIGNED_SZ);
                unsigned attrLen = offsetField - startRecData - recLen;
                if(::strcmp(a.name.c_str(),attributeName.c_str()) == 0){
                    //attr found
                    void *tmp = ::calloc(1,attrLen + 1 + UNSIGNED_SZ);
                    auto * dataC = static_cast<unsigned char*>(tmp);
                    dataC++;
                    if(a.type == 2){
                        //varchar type so append length
                        ::memcpy(dataC,&attrLen,UNSIGNED_SZ);
                        dataC += UNSIGNED_SZ;
                    }
                    memcpy(dataC,pageData + offsetLen + recLen,attrLen);
                    memcpy(data,dataC - 1,len + 1);
                    free(tmp);
                    break;
                }
                recLen += len;
            }
            pageData += UNSIGNED_SZ;
            offsetLen -= UNSIGNED_SZ;
        }
        free(temp);
        return 0;
    }

        RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                        const std::vector<std::string> &attributeNames,
                                        RBFM_ScanIterator &rbfm_ScanIterator) {
            RID rid;
            rid.pageNum = 0;
            rid.slotNum = 1;
            //fileHandle.readPage(rid.pageNum,rbfm_ScanIterator.currPage);
            rbfm_ScanIterator.currRid = rid;
            rbfm_ScanIterator.fileHandle = fileHandle;
            rbfm_ScanIterator.recordDescriptor = recordDescriptor;
            rbfm_ScanIterator.compOp = compOp;
            rbfm_ScanIterator.conditionAttr = conditionAttribute;
            rbfm_ScanIterator.value = &value;
            rbfm_ScanIterator.attributeNames = attributeNames;
            return -1;
        }

        RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
            RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
            void *attrData = ::malloc(PAGE_SIZE);
//            rbfm.readAttribute(this->fileHandle, this->recordDescriptor, this->currRid, this->conditionAttr, attrData);
//            auto *temp = static_cast<unsigned char*>(attrData);
//            unsigned pos = getAttrNum(this->conditionAttr,this->recordDescriptor);
//            unsigned len = UNSIGNED_SZ;
//            if(this->recordDescriptor[pos].type == 2){
//                ::memcpy(&len,++temp, UNSIGNED_SZ);
//                temp += UNSIGNED_SZ;
//            }

            bool flag = true;
            while(flag){
                rbfm.readAttribute(this->fileHandle, this->recordDescriptor, this->currRid, this->conditionAttr, attrData);
                auto *temp = static_cast<unsigned char*>(attrData);
                unsigned pos = getAttrNum(this->conditionAttr,this->recordDescriptor);
                unsigned len = UNSIGNED_SZ;
                if(this->recordDescriptor[pos].type == 2){
                    ::memcpy(&len,++temp, UNSIGNED_SZ);
                    temp += UNSIGNED_SZ;
                }
                if (::memcmp(temp, this->value, len) == 0) {
                    rid = currRid;
                    //get other attributes of this rid
                    flag = false;
                }else{
                    this->currRid.slotNum++;
                    //check if all records scanned
                    unsigned numRec = 0;
                    if(this->currRid.slotNum > numRec){
                        this->currRid.pageNum++;
                    }
                    if(this->currRid.pageNum == fileHandle.numPages)
                        flag = false;
                }
            }

            return RBFM_EOF;
        }


}