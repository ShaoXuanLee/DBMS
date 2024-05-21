#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

//Insert a tuple with the given attribute values (in attrList) in relation. 
//The value of the attribute is supplied in the attrValue member of the attrInfo structure. Since the order of the attributes in attrList[] may not be the same 
//as in the relation, you might have to rearrange them before insertion. 
//If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs.


const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	// part 6
	//check for nulls
	for (int i = 0; i < attrCnt; i++) {
		if (attrList[i].attrValue == NULL) {
			return ATTRNOTFOUND;
		}
	}
	Status status;
	//first check that attrCnt corresponds the relation attribute count
	RelDesc rel_desc;
	//fetch the structure of relation into reldesc
	status = relCat->getInfo(relation, rel_desc);
	if (status != OK) return status; 
	//compare attrcnt
	if (rel_desc.attrCnt != attrCnt) {
		return ATTRNOTFOUND;
	}
	
	AttrDesc * attrs_desc;
	//number of attribute counts
	int relAttrCnt;
	status = attrCat->getRelInfo(relation, relAttrCnt, attrs_desc);
	if (status != OK) return status; 
	
	RID outRID;
	Record outputRecord;
	int tempINT;
	float tempFLOAT;
	

	//initialize record
	int total_len = 0;
	for(int i = 0; i < attrCnt; i++){
		total_len += attrs_desc[i].attrLen;
	}
	char outputData[total_len];
	outputRecord.data = (void*) outputData;
	outputRecord.length = total_len;

	char* finalData;
	int * intaddr;
	float* floataddr;
	for(int i = 0; i < relAttrCnt; i++){
		for(int j = 0; j < attrCnt; j++){

			if(strcmp(attrs_desc[j].attrName, attrList[i].attrName) == 0){ // matching found
				switch(attrList[j].attrType){
					case INTEGER:
					//dst, src, size
						tempINT = atoi((char*)attrList[j].attrValue);
						intaddr = &tempINT;
						finalData = (char*)intaddr;
						break;

					case FLOAT:
						tempFLOAT = atof((char*)attrList[j].attrValue);
						floataddr = &tempFLOAT;
						finalData = (char*)floataddr;
						break;
					
					case STRING:
						finalData = (char*)(attrList[j].attrValue);
						break;
			}
				memcpy((char*)outputRecord.data + attrs_desc[i].attrOffset, finalData, attrs_desc[i].attrLen);
				break;
			}
		}
	}

	InsertFileScan *ifs = new InsertFileScan(relation, status);
	if (status != OK) return status; 
	status = ifs->insertRecord(outputRecord, outRID);
	if (status != OK) return status; 
	return OK;

}

