#include <cmath>
#include "src/include/rm.h"
#include <cstring>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    static unsigned maxTableId;

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    //implementing rm Table class functions
    unsigned Table::getTableId(){
        return this->tableId;
    }
    TableType Table::getTableType() {
        return this->tableType;
    }
    std::string Table::getTableName() {
        return this->tableName;
    }
    std::string Table::getFileName(){
        return this->fileName;
    }
    std::vector<Attribute> Table::getRecordDesc(){
        return this->recordDesc;
    }
    void Table::setTableId(unsigned tableId) {
        this->tableId = tableId;
    }

    void Table::setRecDesc(const std::vector<Attribute> &attrs){
        if(this->getTableType() == User){
            this->recordDesc = attrs;
            return;
        }
        std::string tableName = this->getTableName();
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
            tableType.type = TypeVarChar;
            tableType.length = VARCHAR_SZ;
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
        //return 0;
    }
    unsigned createTablesData(void *data, Table table){
        unsigned tableId = table.getTableId();
        std::string tableName = table.getTableName();
        std::string fileName = table.getFileName();
        TableType tableType = table.getTableType();
        unsigned lenTableName = tableName.length();
        unsigned lenFileName = fileName.length();

        auto * dataC = static_cast<unsigned char *>(calloc(1,PAGE_SIZE));
        unsigned recSz = 0;
        unsigned numFields = table.getRecordDesc().size();
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
//        free(dataC);
        return recSz;
    }

    unsigned createColumnsData(void * data, unsigned tableId, Attribute a, unsigned pos, unsigned numFields){
        std::string colName = a.name;
        unsigned colType = a.type;
        unsigned len = a.length;
        unsigned lenColName = colName.length();

        auto* dataC = static_cast<unsigned char*>(calloc(1,PAGE_SIZE));
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
        return 0;
    }

    RC RelationManager::createCatalog() {
        //create system tables: tables and columns
        //create files
        //add data in these tables
        Table table(TABLES,System);
        Table column(COLUMNS,System);
        if(rbfm.createFile(TABLES) == -1)
            return -1;

        if(rbfm.createFile(COLUMNS) == -1)
            return -1;

        FileHandle tableFileHandle;
        if(rbfm.openFile(table.getFileName(),tableFileHandle) == -1)
            return -1;
        const std::vector<Attribute> vec;
        table.setRecDesc(vec);
        std::vector<Attribute> tableRecDesc = table.getRecordDesc();
        table.setTableId(1);

        column.setRecDesc(vec);
        std::vector<Attribute> colRecordDescriptor = column.getRecordDesc();
        column.setTableId(2);

        RID rid;
        void *data = calloc(1, PAGE_SIZE);
        createTablesData(data, table);
        rbfm.insertRecord(tableFileHandle,tableRecDesc, data,rid);

        createTablesData(data,column);
        rbfm.insertRecord(tableFileHandle,colRecordDescriptor, data,rid);

        FileHandle colFileHandle;
        if(rbfm.openFile(COLUMNS,colFileHandle) == -1)
            return -1;

        //tables
        unsigned tablesFields = tableRecDesc.size();
        for(unsigned i = 0; i < tablesFields; i++){
            createColumnsData(data,table.getTableId(),tableRecDesc[i],i+1,tablesFields);
            rbfm.insertRecord(colFileHandle,colRecordDescriptor,data,rid);
        }

        //columns
        unsigned colFields = colRecordDescriptor.size();
        for(unsigned i = 0; i < colFields; i++){
            createColumnsData(data,column.getTableId(),colRecordDescriptor[i],i+1,tablesFields);
            rbfm.insertRecord(colFileHandle,colRecordDescriptor,data,rid);
        }

        free(data);
        maxTableId = 2;
        rbfm.closeFile(tableFileHandle);
        rbfm.closeFile(colFileHandle);
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        //delete catalog files of the 2 system tables: tables and columns
        //rbfm funcs: destroyfile
        if(rbfm.destroyFile(TABLES) == -1)
            return -1;
        if(rbfm.destroyFile(COLUMNS) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        //create a file named tablename
        //update the catalog tables with req info
        //rbfm funcs needed: createfile, insert record

        //check if catalog tables exist
        FileHandle tableFileHandle;
        if(rbfm.openFile(TABLES,tableFileHandle) == -1)
            return -1;

        FileHandle colFileHandle;
        if(rbfm.openFile(COLUMNS,colFileHandle) == -1)
            return -1;

        if(rbfm.createFile(tableName) == -1)
            return -1;

        Table table(TABLES,System);
        table.setRecDesc(attrs);

        Table newTable(tableName,User);
        newTable.setTableId(++maxTableId);
        newTable.setRecDesc(attrs);

        void *data = calloc(1,PAGE_SIZE);
        createTablesData(data, newTable);
        RID rid;
        rbfm.insertRecord(tableFileHandle,table.getRecordDesc(), data,rid);

        Table column(COLUMNS,System);
        column.setRecDesc(attrs);
        std::vector<Attribute> colRecDesc = column.getRecordDesc();

        //tables
        unsigned numFields = attrs.size();
        for(unsigned i = 0; i < numFields; i++){
            createColumnsData(data, maxTableId,attrs[i],i+1,numFields);
            rbfm.insertRecord(colFileHandle,colRecDesc,data,rid);
        }
        rbfm.closeFile(tableFileHandle);
        rbfm.closeFile(colFileHandle);
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        //delete the file corresponding to the table
        //update the catalog tables to delete the entry for that table
        //rbfm func: destroyfile, deleteRecord
        if(rbfm.destroyFile(tableName) == -1)
            return -1;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        //scan catalog tables to get attributes of a table
        //openFile, readRecord
        Table userTable(tableName,User);

        FileHandle tableFileHandle;
        if(rbfm.openFile(TABLES,tableFileHandle) == -1)
            return -1;

        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        //add a record in the corresponding table
        //using the catalog tables find the table file name and get relevant columns and create a record descriptor that can be passed
        //openfile, insertRecord
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        //use the catalog tables to find the file for the table
        //using the column table create a record descriptor for the attributes
        //openfile, deleteRecord
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        //find the table file name
        //create the record descriptor from the columns table
        //openfile, readrecord
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        //use print record function to do this
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

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