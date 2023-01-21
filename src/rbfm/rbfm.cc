#include "src/include/rbfm.h"
#include <cstdio>
#include <cmath>
#include <cstring>
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

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        unsigned numFields = recordDescriptor.size();
        unsigned numBytes = ceil(numFields / 8);

        unsigned recSize = 0;
        for(Attribute a: recordDescriptor){
            recSize += a.length;
        }

        void *nullP = malloc(numBytes);
        memcpy(nullP, data, numBytes);
        return 0;
    }

    void findFreePage(unsigned recSize, FileHandle &fileHandle){
        FILE *fp = fileHandle.fileP;
        void *data = ::malloc(PAGE_SIZE);
        fread(data,PAGE_SIZE, 1, fp);
        auto *dataB = static_cast<unsigned int*>(data);
        dataB += (PAGE_SIZE / UNSIGNED_SZ) - 1;

        void *freeD = ::malloc(UNSIGNED_SZ);
        memcpy(freeD,dataB,UNSIGNED_SZ);
        unsigned freeS = *(static_cast<unsigned int*>(freeD));

        if(freeS > recSize)
            return;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {

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

