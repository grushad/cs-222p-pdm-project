#include "src/include/rbfm.h"
#include <cstdio>
#include <cmath>
#include <cstring>
#include <iostream>
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
        if(pfm.createFile(fileName) == -1)
            return -1;
        FILE *fp = fopen(fileName.c_str(),"w");
        fseek(fp, UNSIGNED_SZ, SEEK_END);
        unsigned int ps = PAGE_SIZE;
        unsigned int* freeP = &ps;
        fwrite(freeP, UNSIGNED_SZ,1,fp);
        fclose(fp);
        freeP = nullptr;
        return 0;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if(pfm.openFile(fileName,fileHandle) == -1)
            return -1;
        FILE *fp = fileHandle.fileP;
        void *data = ::malloc(PAGE_SIZE);
        fread(data,PAGE_SIZE, 1, fp);

        auto *buffer= static_cast<unsigned int*>(data);
        buffer += (PAGE_SIZE / UNSIGNED_SZ) - 2;

        void *numR = ::malloc(UNSIGNED_SZ);
        memcpy(numR, buffer, UNSIGNED_SZ);
        fileHandle.numRec = *(static_cast<unsigned int*>(numR));

        buffer +=1;

        void *numF = ::malloc(UNSIGNED_SZ);
        memcpy(numR, buffer, UNSIGNED_SZ);
        fileHandle.numFree = *(static_cast<unsigned int*>(numF));

        free(numR);
        free(numF);
        free(data);

        return 0;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        if(fileHandle.fileP != nullptr){
            unsigned int *numR = &fileHandle.numRec;
            ::fseek(fileHandle.fileP,2 * UNSIGNED_SZ ,SEEK_END);
            ::fwrite(numR,UNSIGNED_SZ,1,fileHandle.fileP);

            unsigned int *numF = &fileHandle.numFree;
            ::fwrite(numF,UNSIGNED_SZ,1,fileHandle.fileP);

            pfm.closeFile(fileHandle);
            return 0;
        }
         return -1;
    }

    bool checkPageFreeSpace(unsigned recSize, FILE **fp){
        void *data = ::malloc(PAGE_SIZE);
        fread(data,PAGE_SIZE, 1, *fp);
        auto *dataB = static_cast<unsigned int*>(data);
        dataB += (PAGE_SIZE / UNSIGNED_SZ) - 1;

        void *freeD = ::malloc(UNSIGNED_SZ);
        memcpy(freeD,dataB,UNSIGNED_SZ);
        unsigned freeS = *(static_cast<unsigned int*>(freeD));

        free(data);
        if(freeS > recSize)
            return true;

        return false;
    }

    unsigned findFreePage(unsigned recSize, FileHandle &fileHandle){
        FILE *fp = fileHandle.fileP;
        if(checkPageFreeSpace(recSize, &fp)) {
            unsigned numP = (ftell(fp) - PAGE_SIZE)/ PAGE_SIZE;
            if(numP != 0)
                return numP - 1;
        }
        //skip the hidden page
        fseek(fp,PAGE_SIZE,SEEK_SET);
        for(unsigned i = 0; i < fileHandle.numPages; i++){
            if(checkPageFreeSpace(recSize, &fp))
                return i;
            fseek(fp,PAGE_SIZE,SEEK_CUR);
        }
        return fileHandle.numPages;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        unsigned numFields = recordDescriptor.size();
        unsigned nullInd = ceil(numFields / 8);

        unsigned recSize = 0;
        for(Attribute a: recordDescriptor){
            recSize += a.length;
        }

        unsigned totalRecSize = UNSIGNED_SZ + nullInd + (numFields * UNSIGNED_SZ) + recSize;
        unsigned pageNum = findFreePage(totalRecSize, fileHandle);

        auto *recData = static_cast<unsigned char*>(::malloc(totalRecSize));
        ::memcpy(recData,&numFields,UNSIGNED_SZ);

        recData += UNSIGNED_SZ;
        ::memcpy(recData, data, nullInd);

        recData += nullInd;

        void *nullPtr = malloc(numFields);
        ::memcpy(nullPtr,data, numFields);

        unsigned offsetBytes = totalRecSize - recSize;
        for(int i = 0; i < numFields; i++){
            Attribute a = recordDescriptor[i];
            if(((unsigned char *) nullPtr) [i] == 1){
                signed np = -1;
                ::memcpy(recData, &np, UNSIGNED_SZ);
            }else{
                offsetBytes += a.length;
                ::memcpy(recData, &offsetBytes, UNSIGNED_SZ);
            }
            recData += UNSIGNED_SZ;
        }
        void *temp = ::malloc(sizeof(data));
        ::memcpy(temp, data, sizeof(data));
        auto *content = static_cast<unsigned char*>(temp);

        content += nullInd;

        for(int i = 0; i < numFields; i++){
            Attribute a = recordDescriptor[i];
            if(((unsigned char *) nullPtr) [i] == 0){
                if(a.type == 2){
                    content += UNSIGNED_SZ;
                }
                ::memcpy(recData, content, a.length);
                recData += a.length;
            }
        }

        rid.pageNum = pageNum;
//        fileHandle.numRec += 1;
        rid.slotNum = fileHandle.numRec;

        if(pageNum == fileHandle.numPages){
            fileHandle.appendPage(recData);
        }else{
            fileHandle.writePage(pageNum, recData);
        }
//        FILE *fp = fileHandle.fileP;
//        void * readP = ::malloc(PAGE_SIZE);
//        fileHandle.readPage(pageNum, readP);
//
//        auto *readT = static_cast<unsigned int*>(readP);
//        readT += (PAGE_SIZE / UNSIGNED_SZ) - 2;
//
//        void * slotD = ::malloc(2 * UNSIGNED_SZ);
//        unsigned skipB = ((2 + (fileHandle.numRec * 2)) * UNSIGNED_SZ);
//        ::memcpy(slotD, &fileHandle.numRec,)
        return 0;
    }


    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        if(fileHandle.fileP == nullptr)
            return -1;

        unsigned pageNum = rid.pageNum;
        unsigned slotNum = rid.slotNum;

        void * content = malloc(PAGE_SIZE);
        fileHandle.readPage(pageNum, content);

        auto *temp = static_cast<unsigned int*>(content);
        temp += PAGE_SIZE / UNSIGNED_SZ;
        temp -=2;
        temp -= (slotNum * 2);
        void * offL = ::malloc(UNSIGNED_SZ * 2);
        ::memcpy(offL, temp, UNSIGNED_SZ * 2);
        auto *tmp = static_cast<unsigned int*>(offL);
        unsigned len = ((unsigned int * )tmp)[0];
        unsigned offset = ((unsigned int * )tmp)[1];

        auto *temp2 = static_cast<unsigned char*>(content);
        temp2 += offset;
        memcpy(data,temp2,len);

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

