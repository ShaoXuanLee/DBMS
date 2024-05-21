#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	Status status;

	// declare AttrDesc array for ScanSelect
	AttrDesc projectNames[projCnt];

	// declare attrDesc
	AttrDesc attrDesc;
	int reclen = 0;

	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projectNames[i]);
		reclen += projectNames[i].attrLen;
	}

	if (attr != NULL) {
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
	}

	status = ScanSelect(result, projCnt, projectNames, &attrDesc, op, attrValue, reclen);
	
	return OK;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	char outData[reclen];
	Record outRec;
	outRec.data = (void*) outData;
	outRec.length = reclen;

	int intFilterVal;
	float floatFilterVal;
	Status status;
	Record rec;
	RID rid;

	InsertFileScan *ifs = new InsertFileScan(result, status);
	HeapFileScan *hfs = new HeapFileScan(projNames[0].relName, status);

	// unconditional scan if attrDesc is NULL
	if (attrDesc == NULL) {
		status = hfs->startScan(0, 0, STRING, NULL, op);
	} else {
		switch (attrDesc->attrType) {
			case STRING:
				status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
				break;

			case FLOAT:
				floatFilterVal = atof(filter); // convert filter into a float
				status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char *) &floatFilterVal, op);
				break;

			case INTEGER:
				intFilterVal = atoi(filter); // convert filter into a float
				status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char *) &intFilterVal, op);
				break;
		}
	}

	while ((status = hfs->scanNext(rid)) == OK) {
		status = hfs->getRecord(rec);
		int offset = 0;
		RID outRID;

		// copy selected attribute values from scan into data buffer of output record
		for (int i = 0; i < projCnt; i++) {
			memcpy(outData + offset, (char*) rec.data + projNames[i].attrOffset, projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}

		status = ifs->insertRecord(outRec, outRID);
		if (status != OK) { 
            return status;
        }
	}

	delete hfs;
	delete ifs;

	return status;
}
