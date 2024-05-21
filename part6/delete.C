#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
/*
This function will delete all tuples in relation satisfying the predicate specified by attrName, 
op, and the constant attrValue. type denotes the type of the attribute. 
You can locate all the qualifying tuples using a filtered HeapFileScan.
*/	
	Status status;

	//with the relation, get the attributes of the relation
	AttrDesc attr_desc;
	AttrCatalog A = AttrCatalog(status);
	A.getInfo(relation, attrName, attr_desc);
	if (status != OK) return status;

	HeapFileScan hfs = HeapFileScan(relation, status);
	if (status != OK) return status;

	//scan through attr based on type
	int tempint = 0;
	float tempfloat = 0;
	switch(type){
		case INTEGER:
			tempint = atoi(attrValue);
			status = hfs.startScan(attr_desc.attrOffset, attr_desc.attrLen, type, (char*)&tempint, op);
			if (status != OK) return status;
			break;

		case FLOAT:
			tempfloat = atof(attrValue);
			status = hfs.startScan(attr_desc.attrOffset, attr_desc.attrLen, type, (char*)&tempfloat, op);
			if (status != OK) return status;
			break;

		case STRING:
			status = hfs.startScan(attr_desc.attrOffset, attr_desc.attrLen, type, attrValue, op);
			if (status != OK) return status;
			break;
	}
	//at this point, the data member of hfs is filled
	// now delete them from the relation
	RID nextRID;
	status = hfs.scanNext(nextRID);
	while(status == OK){
		status = hfs.deleteRecord();
		if(status != OK){
			hfs.endScan();
			return status;
		}
		status = hfs.scanNext(nextRID);
	}
	hfs.endScan();
	return OK;
}


