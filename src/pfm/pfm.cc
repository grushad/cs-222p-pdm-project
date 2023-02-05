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
            if(data == nullptr)
                return -1;
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

            if(file == nullptr)
                return -1;

            auto *bufferA = static_cast<unsigned *>(malloc(PAGE_SIZE));
            if(bufferA == nullptr)
                return -1;

            fread(bufferA, PAGE_SIZE, 1, file);
            fileHandle.fileP = file;

            fileHandle.numPages = bufferA[0];
            fileHandle.readPageCounter = bufferA[1];
            fileHandle.writePageCounter = bufferA[2];
            fileHandle.appendPageCounter = bufferA[3];
            free(bufferA);
            return 0;
        }
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.fileP != nullptr) {
            rewind(fileHandle.fileP);
            void *firstPage = calloc(1,PAGE_SIZE);
            fread(firstPage,1,PAGE_SIZE,fileHandle.fileP);
            auto * firstP = static_cast<unsigned *>(firstPage);
            firstP[0] = fileHandle.numPages;
            firstP[1] = fileHandle.readPageCounter;
            firstP[2] = fileHandle.writePageCounter;
            firstP[3] = fileHandle.appendPageCounter;
            rewind(fileHandle.fileP);
            ::fwrite(firstP,PAGE_SIZE,1,fileHandle.fileP);
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
            if(this->numPages <= pageNum){
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
            if(this->numPages <= pageNum){
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