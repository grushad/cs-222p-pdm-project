#include <cmath>
#include "src/include/rm.h"
#include <cstring>

namespace PeterDB {
    static unsigned maxTableId;
    RelationManager &RelationManager::instance() {
        maxTableId = 0;
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    //implementing rm Table class functions
    unsigned TableManager::getTableId(){
        return this->tableId;
    }
    TableType TableManager::getTableType() {
        return this->tableType;
    }
    std::string TableManager::getTableName() {
        return this->tableName;
    }
    std::string TableManager::getFileName(){
        return this->fileName;
    }
    std::vector<Attribute> TableManager::getRecordDesc(){
        return this->recordDesc;
    }
    unsigned TableManager::getNextTableId() {
        return maxTableId++;
    }

    void TableManager::setCatalogRecDesc(){
        const std::string tableName = this->getTableName();
        if(strcmp(tableName.c_str(),TABLES) == 0){
            //create record desc for tables
            Attribute tableId;
            tableId.name = "table-id";
            tableId.type = TypeInt;
            tableId.length = UNSIGNED_SZ;
            this->recordDesc.push_back(tableId);

            Attribute tableN;
            tableN.name = "table-name";
            tableN.type = TypeVarChar;
            tableN.length = VARCHAR_SZ;
            this->recordDesc.push_back(tableN);

            Attribute fileName;
            fileName.name = "file-name";
            fileName.type = TypeVarChar;
            fileName.length = VARCHAR_SZ;
            this->recordDesc.push_back(fileName);

            Attribute tableType;
            tableType.name = "table-type";
            tableType.type = TypeInt;
            tableType.length = UNSIGNED_SZ;
            this->recordDesc.push_back(tableType);

        }else{
            //create for columns
            //create record desc for tables
            Attribute tableId;
            tableId.name = "table-id";
            tableId.type = TypeInt;
            tableId.length = UNSIGNED_SZ;
            this->recordDesc.push_back(tableId);

            Attribute columnName;
            columnName.name = "column-name";
            columnName.type = TypeVarChar;
            columnName.length = VARCHAR_SZ;
            this->recordDesc.push_back(columnName);

            Attribute colType;
            colType.name = "column-type";
            colType.type = TypeInt;
            colType.length = UNSIGNED_SZ;
            this->recordDesc.push_back(colType);

            Attribute colLen;
            colLen.name = "column-length";
            colLen.type = TypeInt;
            colLen.length = UNSIGNED_SZ;
            this->recordDesc.push_back(colLen);

            Attribute colPos;
            colPos.name = "column-position";
            colPos.type = TypeInt;
            colPos.length = UNSIGNED_SZ;
            this->recordDesc.push_back(colPos);
        }
    }
    unsigned createTablesData(void *data, TableManager table){
        unsigned tableId = table.getTableId();
        std::string tableName = table.getTableName();
        std::string fileName = table.getFileName();
        TableType tableType = table.getTableType();
        unsigned lenTableName = tableName.length();
        unsigned lenFileName = fileName.length();

        void * temp = malloc(PAGE_SIZE);
        if(temp == nullptr)
            return -1;
        auto * dataC = static_cast<unsigned char *>(temp);

        unsigned recSz = 0;
        unsigned numFields = 4;
        unsigned nullBytes = ceil((double)numFields / 8);
        dataC += nullBytes; //skipping null fields as there are none;
        recSz += nullBytes;

        memcpy(dataC,&tableId,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,&lenTableName,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,tableName.c_str(),lenTableName);
        recSz += lenTableName;
        dataC += lenTableName;

        memcpy(dataC,&lenFileName,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,fileName.c_str(),lenFileName);
        recSz += lenFileName;
        dataC += lenFileName;

        memcpy(dataC,&tableType,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(data, dataC - recSz,recSz);
//        free(temp);
        return recSz;
    }

    unsigned createColumnsData(void * data, unsigned tableId, Attribute a, unsigned pos, unsigned numFields){
        std::string colName = a.name;
        unsigned colType = a.type;
        unsigned len = a.length;
        unsigned lenColName = colName.length();

        void * temp = malloc(PAGE_SIZE);
        if(temp == nullptr)
            return -1;
        auto* dataC = static_cast<unsigned char*>(temp);
        unsigned recSz = 0;
        unsigned nullBytes = ceil((double)numFields / 8);
        dataC += nullBytes; //skipping the null bytes as none fields are null
        recSz += nullBytes;

        memcpy(dataC,&tableId,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,&lenColName,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,colName.c_str(),lenColName);
        recSz += lenColName;
        dataC += lenColName;

        memcpy(dataC,&colType,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,&len,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(dataC,&pos,UNSIGNED_SZ);
        recSz += UNSIGNED_SZ;
        dataC += UNSIGNED_SZ;

        memcpy(data,dataC - recSz,recSz);
//        free(temp);
        return 0;
    }

    bool rmfileExists(const std::string &name){
        if (FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            return true;
        } else {
            return false;
        }
    }

    RC RelationManager::createCatalog() {
        if(rmfileExists(TABLES) || rmfileExists(COLUMNS))
            return -1;
        const std::vector<Attribute> vec;
        TableManager table(TABLES, vec);
        TableManager column(COLUMNS, vec);

        rbfm.createFile(TABLES);
        rbfm.createFile(COLUMNS);

        FileHandle tableFileHandle;
        if(rbfm.openFile(TABLES,tableFileHandle) == -1)
            return -1;

        const std::vector<Attribute> tableRecDesc = table.getRecordDesc();
        const std::vector<Attribute> colRecordDescriptor = column.getRecordDesc();

        RID rid;
        void *data = malloc( PAGE_SIZE);
        if(data == nullptr)
            return -1;
        createTablesData(data, table);
        rbfm.insertRecord(tableFileHandle,tableRecDesc, data,rid);

        createTablesData(data,column);
        rbfm.insertRecord(tableFileHandle,tableRecDesc, data,rid);

        FileHandle colFileHandle;
        if(rbfm.openFile(COLUMNS,colFileHandle) == -1)
            return -1;

        //tables
        unsigned tablesFields = tableRecDesc.size();
        unsigned colFields = colRecordDescriptor.size();
        for(unsigned i = 0; i < tablesFields; i++){
            createColumnsData(data,table.getTableId(),tableRecDesc[i],i+1,colFields);
            rbfm.insertRecord(colFileHandle,colRecordDescriptor,data,rid);
        }

        //columns
        for(unsigned i = 0; i < colFields; i++){
            createColumnsData(data,column.getTableId(),colRecordDescriptor[i],i+1,colFields);
            rbfm.insertRecord(colFileHandle,colRecordDescriptor,data,rid);
        }

//        free(data);
        rbfm.closeFile(tableFileHandle);
        rbfm.closeFile(colFileHandle);
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        if(rbfm.destroyFile(TABLES) == -1)
            return -1;
        if(rbfm.destroyFile(COLUMNS) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        FileHandle tableFileHandle;
        if(rbfm.openFile(TABLES,tableFileHandle) == -1)
            return -1;
        FileHandle colFileHandle;
        if(rbfm.openFile(COLUMNS,colFileHandle) == -1)
            return -1;
        if(rbfm.createFile(tableName) == -1)
            return -1;

        TableManager table(TABLES,attrs);
        TableManager newTable(tableName,attrs);

        void *data = malloc(PAGE_SIZE);
        if(data == nullptr)
            return -1;
        createTablesData(data, newTable);
        RID rid;
        rbfm.insertRecord(tableFileHandle,table.getRecordDesc(), data,rid);

        TableManager column(COLUMNS,attrs);
        const std::vector<Attribute> colRecDesc = column.getRecordDesc();

        //columns
        unsigned numFields = attrs.size();
        for(unsigned i = 0; i < numFields; i++){
            createColumnsData(data, newTable.getTableId(),attrs[i],i+1,colRecDesc.size());
            rbfm.insertRecord(colFileHandle,colRecDesc,data,rid);
        }
        rbfm.closeFile(tableFileHandle);
        rbfm.closeFile(colFileHandle);
//        free(data);
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(strcmp(tableName.c_str(),TABLES) == 0 || strcmp(tableName.c_str(),COLUMNS) == 0)
            return -1;
        if(rbfm.destroyFile(tableName) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        FileHandle tableFileHandle;
        if(rbfm.openFile(TABLES,tableFileHandle) == -1)
            return -1;
        TableManager table(TABLES,attrs);

        FileHandle colFileHandle;
        if(rbfm.openFile(COLUMNS,colFileHandle) == -1)
            return -1;
        TableManager column(COLUMNS,attrs);

        if(!rmfileExists(tableName))
            return -1;

        if(strcmp(tableName.c_str(),table.getTableName().c_str()) == 0) {
            attrs = table.getRecordDesc();
            return 0;
        }

        if(strcmp(tableName.c_str(),column.getTableName().c_str()) == 0) {
            attrs = column.getRecordDesc();
            return 0;
        }

        //find table id for this table
        RID rid;
        rid.pageNum = 0;
        rid.slotNum = 1;

        unsigned totalPages = tableFileHandle.numPages;

        std::string currTableName;
        std::vector<Attribute> tableRecDesc = table.getRecordDesc();
        unsigned nullBytes = ceil((double) tableRecDesc.size() / 8);
        unsigned tableId = 3;
        void * temp = malloc(PAGE_SIZE);
        if(temp == nullptr)
            return -1;
        auto *data = static_cast<unsigned char*>(temp);

        while(rid.pageNum < totalPages && strcmp(currTableName.c_str(), tableName.c_str()) != 0){
            RC rc = rbfm.readRecord(tableFileHandle,tableRecDesc,rid,data);
            if(rc == -1)
                return -1;
            if(rc == -2){
                rid.pageNum++;
                rid.slotNum = 1;
            }else {
                //record in API format
                data += nullBytes; //skip null bytes and 4 bytes for table id
                memcpy(&tableId,data,UNSIGNED_SZ);
                data += UNSIGNED_SZ;
                unsigned len;
                memcpy(&len, data, UNSIGNED_SZ);
                data += UNSIGNED_SZ; //skipping length field for table name
                std::string name(reinterpret_cast<char const *>(data), len);
                currTableName = name;
                rid.slotNum++;
                data -= (2 * UNSIGNED_SZ);
                data -= nullBytes;
            }
        }

        std::vector<Attribute> colRecDesc = column.getRecordDesc();
        nullBytes = ceil((double) colRecDesc.size() / 8);
        rid.pageNum = 0;
        rid.slotNum = 1;
        bool found = false;
        if(strcmp(currTableName.c_str(),tableName.c_str()) == 0){
            //search in columns table
            while(rid.pageNum < colFileHandle.numPages){
                RC rc = rbfm.readRecord(colFileHandle,colRecDesc,rid,data);
                if(rc == -1)
                    return -1;
                if(rc == -2){
                    rid.pageNum++;
                    rid.slotNum = 1;
                }else {
                    //record in API format
                    data += nullBytes; //skip null bytes
                    unsigned curId;
                    memcpy(&curId,data,UNSIGNED_SZ);
                    data += UNSIGNED_SZ; //skipping table id field
                    if(curId == tableId){
                        found = true;
                        Attribute a;
                        unsigned lenName;
                        memcpy(&lenName,data,UNSIGNED_SZ);
                        data += UNSIGNED_SZ;//skipping length field

                        std::string colName(reinterpret_cast<char const *>(data), lenName);
                        a.name = colName;
                        data += lenName;
                        memcpy(&a.type,data,UNSIGNED_SZ);

                        data += UNSIGNED_SZ;
                        memcpy(&a.length,data, UNSIGNED_SZ);
                        attrs.push_back(a);
                        data -= (2 * UNSIGNED_SZ);
                        data -= lenName;
                    }else{
                        if(found){
                            break;
                        }
                    }
                    data -= UNSIGNED_SZ;
                    data -= nullBytes;
                    rid.slotNum++;
                }
            }
        }else{
            return -1;
        }
        rbfm.closeFile(tableFileHandle);
        rbfm.closeFile(colFileHandle);
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(strcmp(tableName.c_str(),TABLES) == 0 || strcmp(tableName.c_str(),COLUMNS) == 0)
            return -1;
        FileHandle fileHandle;
        if(rbfm.openFile(tableName,fileHandle) == -1)
            return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        if(rbfm.insertRecord(fileHandle,recordDescriptor,data,rid) == -1)
            return -1;
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(strcmp(tableName.c_str(),TABLES) == 0 || strcmp(tableName.c_str(),COLUMNS) == 0)
            return -1;
        FileHandle fileHandle;
        if(rbfm.openFile(tableName,fileHandle) == -1)
            return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        if(rbfm.deleteRecord(fileHandle,recordDescriptor,rid) == -1)
            return -1;
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if(strcmp(tableName.c_str(),TABLES) == 0 || strcmp(tableName.c_str(),COLUMNS) == 0)
            return -1;
        FileHandle fileHandle;
        if(rbfm.openFile(tableName,fileHandle) == -1)
            return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        if(rbfm.updateRecord(fileHandle,recordDescriptor,data,rid) == -1)
            return -1;
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        FileHandle fileHandle;
        if(rbfm.openFile(tableName,fileHandle) == -1)
            return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        if(rbfm.readRecord(fileHandle,recordDescriptor,rid,data) == -1)
            return -1;
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        //use print record function to do this
        rbfm.printRecord(attrs,data,out);
        return 0;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        FileHandle fileHandle;
        if(rbfm.openFile(tableName,fileHandle) == -1)
            return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName,recordDescriptor);
        if(rbfm.readAttribute(fileHandle,recordDescriptor,rid,attributeName,data) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        std::vector<Attribute> recordDesc;
        getAttributes(tableName,recordDesc);
        TableManager tb(tableName, recordDesc);
        recordDesc = tb.getRecordDesc();
        FileHandle fileHandle;
        if(rbfm.openFile(tb.getFileName(),fileHandle) == -1)
            return -1;
        RBFM_ScanIterator rbfmScanIterator;
        rbfm.scan(fileHandle,recordDesc,conditionAttribute,compOp,value,attributeNames,rbfmScanIterator);
        rm_ScanIterator.rbfmScanIterator = rbfmScanIterator;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC rc = this->rbfmScanIterator.getNextRecord(rid,data);
        return rc == RBFM_EOF ? RM_EOF :rc;
    }

    RC RM_ScanIterator::close() {
        //this->rbfmScanIterator.close();
        //delete this;
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB