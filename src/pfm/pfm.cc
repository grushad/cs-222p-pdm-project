#include "src/include/pfm.h"
#include <cstdio>
#include <cstring>
/*
 * 1st Page of File:
 * number of pages
 * RC: 4bytes
 * WC: 4bytes
 * AC: 4bytes
 */
using namespace std;
namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    bool fileExists(const std::string &fileName){
        FILE *file = fopen(fileName.c_str(),"r");
        return file != nullptr;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        if(!fileExists(fileName)){
            //write basic metadata to file i.e. one page
            FILE *file = fopen(fileName.c_str(), "w");
            void *data = calloc(1, PAGE_SIZE);
            fwrite(data, PAGE_SIZE,1, file);
            fclose(file);
            free(data);
            return 0;
        }
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(remove(fileName.c_str()) != 0){
            return -1;
        }
        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if(fileExists(fileName)){
            FILE *file = fopen(fileName.c_str(), "rb+");

            void *buffer = malloc(PAGE_SIZE);
            fread(buffer, PAGE_SIZE, 1, file);
            fileHandle.fileP = file;

            void *numP = malloc(UNSIGNED_SZ);
            memcpy(numP, buffer, UNSIGNED_SZ);
            fileHandle.numPages = *(static_cast<unsigned int*>(numP));

            auto *bufferA = static_cast<unsigned int*>(buffer);
            bufferA += 1;

            void *rC = ::malloc(UNSIGNED_SZ);
            memcpy(rC,bufferA,UNSIGNED_SZ);
            fileHandle.readPageCounter = *(static_cast<unsigned int*>(rC));

            bufferA += 1;
            void *wC = malloc(UNSIGNED_SZ);
            memcpy(wC, bufferA, UNSIGNED_SZ);
            fileHandle.writePageCounter = *(static_cast<unsigned int*>(wC));

            bufferA += 1;
            void *aC = malloc(UNSIGNED_SZ);
            ::memcpy(aC, bufferA, UNSIGNED_SZ);
            fileHandle.appendPageCounter = *(static_cast<unsigned int*>(aC));

            free(buffer);
            free(numP);
            return 0;
        }
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.fileP != nullptr) {
            rewind(fileHandle.fileP);
            fwrite(&fileHandle.numPages,UNSIGNED_SZ,1,fileHandle.fileP);
            fwrite(&fileHandle.readPageCounter,UNSIGNED_SZ,1,fileHandle.fileP);
            fwrite(&fileHandle.writePageCounter,UNSIGNED_SZ,1,fileHandle.fileP);
            fwrite(&fileHandle.appendPageCounter,UNSIGNED_SZ,1,fileHandle.fileP);
            fclose(fileHandle.fileP);
            fileHandle.fileP = nullptr;
            return 0;
        }
        return -1;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if(this->fileP != nullptr){
            if(this->numPages < pageNum){
                return -1;
            }
            unsigned seekBytes = (pageNum + 1) * PAGE_SIZE;
            fseek(this->fileP,seekBytes, SEEK_SET);
            fread(data, PAGE_SIZE, 1, this->fileP);
            this->readPageCounter += 1;
            return 0;
        }
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(this->fileP != nullptr){
            if(this->numPages < pageNum){
                return -1;
            }
            unsigned seekBytes = (pageNum + 1) * PAGE_SIZE;
            fseek(this->fileP,seekBytes, SEEK_SET);
            fwrite(data, PAGE_SIZE, 1, this->fileP);
            this->writePageCounter += 1;
            return 0;
        }
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        if(this->fileP != nullptr){
            unsigned seekBytes = (this->numPages + 1) * PAGE_SIZE;
            fseek(this->fileP, seekBytes, SEEK_SET);
            fwrite(data, PAGE_SIZE,1, this->fileP);
            this->appendPageCounter += 1;
            this->numPages += 1;
            return 0;
        }
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        if(this->fileP != nullptr){
            return this->numPages;
        }
        return 0;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        if(this->fileP != nullptr){
            readPageCount = this->readPageCounter;
            writePageCount = this->writePageCounter;
            appendPageCount = this->appendPageCounter;
            return 0;
        }
        return -1;
    }

} // namespace PeterDB