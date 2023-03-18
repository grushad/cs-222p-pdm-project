#include "src/include/qe.h"
#include "src/rbfm/rbfm.cc"
#include <cmath>
#include <map>

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
        this->iter->getAttributes(this->recordDesc);
        getAttrIds(this->recordDesc,this->attrNames,this->attrIdToData);
        this->result = calloc(1,PAGE_SIZE / 2);
    }

    Project::~Project() {
        free(this->result);
        std::map<int, char*>::iterator it;
        for (it = this->attrIdToData.begin(); it != this->attrIdToData.end(); it++){
            free(it->second);
        }
    }

    RC Project::getNextTuple(void *data) {
        RC rc = this->iter->getNextTuple(this->result);
        auto* resultC = static_cast<char *>(this->result);
        if(rc == RM_EOF)
            return QE_EOF;
        std::map<std::string, int> attrNameToId;
        int numFields = this->recordDesc.size();
        int nullBytes = ceil((double) numFields / 8);
        std::vector<bool> nullVec;
        getNullBits(resultC,numFields,nullVec);
        resultC += nullBytes; //skip null bytes
        for(int i = 0; i < numFields; i++){
            if(!nullVec[i]){
                int len = 0;
                if(this->recordDesc[i].type == 2){
                    memcpy(&len, resultC, UNSIGNED_SZ);
                }
                len += UNSIGNED_SZ;
                if(this->attrIdToData.find(i) != this->attrIdToData.end()){
                    attrNameToId[this->recordDesc[i].name] = i;
                    memcpy(attrIdToData[i],resultC,len);
                }
                resultC += len;
            }
        }
        auto* dataC = static_cast<char *>(data);

        int resNullBytes = ceil((double)this->attrNames.size() / 8);
        void* nullData = calloc(1,resNullBytes);
        memcpy(dataC, nullData,resNullBytes);
        dataC += resNullBytes;

        for(const string &attrName: this->attrNames){
            if(attrNameToId.find(attrName) != attrNameToId.end()){
                int id = attrNameToId[attrName];
                int len = 0;
                if(this->recordDesc[id].type == 2){
                    memcpy(&len, this->attrIdToData[id],UNSIGNED_SZ);
                }
                len += UNSIGNED_SZ;
                memcpy(dataC, attrIdToData[id],len);
                dataC += len;
            }
        }
        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        unordered_set<string> names;
        for(const string &s: this->attrNames)
            names.insert(s);
        for(const Attribute &a: this->recordDesc){
            if(names.find(a.name) != names.end())
                attrs.emplace_back(a);
        }
        return 0;
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
        this->iter = input;
        this->aggAttr = aggAttr;
        this->op = op;
        this->iter->getAttributes(this->recordDesc);
        this->result = calloc(1,PAGE_SIZE);
        this->curResI = 0;
        this->curResF = 0.0f;
        this->first = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {
        free(this->result);
    }

    RC Aggregate::getNextTuple(void *data) {
        if(!first)
            first = true;
        else
            return -1;
        while(this->iter->getNextTuple(this->result) != RM_EOF){
            auto* resultC = static_cast<char *>(result);
            int numFields = this->recordDesc.size();
            int nullBytes = ceil((double)numFields / 8);
            vector<bool> nullVec;
            getNullBits(result, numFields, nullVec);
            resultC += nullBytes;
            for(int i = 0; i < this->recordDesc.size(); i++){
                int len = 0;
                if(recordDesc[i].type == 2){
                    memcpy(&len,resultC,UNSIGNED_SZ);
                }
                len += UNSIGNED_SZ;
                if(strcmp(this->recordDesc[i].name.c_str(), this->aggAttr.name.c_str()) == 0){
                    if(!nullVec[i]){
                        if(this->aggAttr.type == 0){ //int
                            int val;
                            memcpy(&val, resultC, UNSIGNED_SZ);
                            switch(this->op){
                                case 0: //min
                                    this->curResI = std::min(this->curResI, val);
                                    break;
                                case 1: //max
                                    this->curResI = std::max(this->curResI, val);
                                    break;
                                case 2: //count
                                    this->curCount++;
                                    break;
                                case 3: //sum
                                    this->curResI += val;
                                    break;
                                case 4: //avg
                                    this->curResI += val;
                                    this->curCount++;
                                    break;
                            }
                        }else if(this->aggAttr.type == 1){ //float
                            float val;
                            memcpy(&val, resultC, UNSIGNED_SZ);
                            switch(this->op){
                                case 0: //min
                                    this->curResF = std::min(this->curResF, val);
                                    break;
                                case 1: //max
                                    this->curResF = std::max(this->curResF, val);
                                    break;
                                case 2: //count
                                    this->curCount++;
                                    break;
                                case 3: //sum
                                    this->curResF += val;
                                    break;
                                case 4: //avg
                                    this->curResF += val;
                                    this->curCount++;
                                    break;
                            }
                        }
                    }
                    break;
                }
                resultC += len;
            }
        }
        if(this->op == 2){
            memcpy(data,&this->curCount,UNSIGNED_SZ);
        }else if(this->aggAttr.type == 0){
            if(this->op == 4){
                float res = (float)this->curResI / (float)this->curCount;
                memcpy(data,&res,UNSIGNED_SZ);
            }else{
                memcpy(data, &this->curResI,UNSIGNED_SZ);
            }

        }else if(this->aggAttr.type == 1){
            if(this->op == 4){
                this->curResF = this->curResF / (float)this->curCount;
            }
            memcpy(data, &this->curResF,UNSIGNED_SZ);
        }
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        for(const Attribute &a: this->recordDesc){
            if(strcmp(a.name.c_str(), this->aggAttr.name.c_str()) == 0) {
                attrs.emplace_back(a);
                break;
            }
        }
        return 0;
    }
} // namespace PeterDB
