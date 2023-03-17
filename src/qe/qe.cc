#include "src/include/qe.h"
#include "src/rbfm/rbfm.cc"
#include <cmath>

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->lhsAttr = condition.lhsAttr;
        this->compOp = condition.op;
        this->value = condition.rhsValue.data;
        this->iter = input;
        this->getAttributes(this->recordDesc);
    }

    Filter::~Filter() {

    }
    int getAttrInd(const std::string &attr,const std::vector<Attribute> &attribute){
        int attrSize = attribute.size();
        for(int i = 0; i < attrSize; i++){
            if(strcmp(attr.c_str(),attribute[i].name.c_str())==0){
                return i;
            }
        }
        return -1;
    }
    RC Filter::getNextTuple(void *data) {
        RC rc = this->iter->getNextTuple(data);
        if(rc == RM_EOF)
            return QE_EOF;

        int pos = getAttrInd(this->lhsAttr,this->recordDesc);
        if(pos == -1)
            return -1;
        int nullBytes = ceil((double) this->recordDesc.size() / 8);
        auto* dataC = static_cast<char *>(data);
        std::vector<bool> nullVec;
        getNullBits(dataC,this->recordDesc.size(),nullVec);
        dataC += nullBytes; //skip null bytes
        if(!nullVec[pos]){
            for(int i = 0; i < pos; i++){
                if(!nullVec[i]){
                    int len = UNSIGNED_SZ;
                    if(this->recordDesc[i].type == 2){
                        memcpy(&len, dataC, UNSIGNED_SZ);
                        dataC += UNSIGNED_SZ;
                    }
                    dataC += len;
                }
            }
            int attrLen = 0, currAttrLen = 0;
            if(this->recordDesc[pos].type == 2){
                memcpy(&attrLen,this->value,UNSIGNED_SZ);
                memcpy(&currAttrLen, dataC, UNSIGNED_SZ);
            }
            attrLen += UNSIGNED_SZ;
            currAttrLen += UNSIGNED_SZ;
            auto* condAttr = static_cast<char *>(calloc(1, attrLen + 1));
            auto* currAttr = static_cast<char *>(calloc(1, currAttrLen + 1));
            memcpy(condAttr + 1, this->value, attrLen);
            memcpy(currAttr + 1, dataC, currAttrLen);

            if(compAttrVal(condAttr,currAttr,this->compOp,this->recordDesc[pos]))
                return 0;
        }
        return this->getNextTuple(data);
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        return this->iter->getAttributes(attrs);
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->iter = input;
        this->attrNames = attrNames;
        this->getAttributes(this->recordDesc);
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return this->iter->getAttributes(attrs);
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
