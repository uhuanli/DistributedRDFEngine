/*
 * DatabaseHBase.h
 *
 *  Created on: 2013-3-31
 *      Author: liyouhuan
 */

#ifndef DATABASEHBASE_H_
#define DATABASEHBASE_H_
#include "Type.h"
#include "Database.h"
#include "HBASE.h"
#include "STREE.h"

class DatabaseHBase:public Database
{
private:
	static std::map<int, std::string> ID2pre;
	static std::map<std::string, int> pre2ID;
	// @liyouhuan
	bool loadPreID();
	int getSubByID(char* _sub, int _subID);
	int getObjByID(char*& _obj, int _objID);
	int getIDBySub(const char* _sub, int &_subID);
	int getIDByObj(const char* _obj, int &_objID);

	int getIDByStr(const char* _str, int &_id, std::string _table);
	int getStrByID(char* _str, int _id, std::string _table);

public:
	// @liyouhuan
	int connect();
	int disconnect();
	// @vstree yago
	int connectRead();
	int disConnectRead();
	int connectWrite();
	int disConnectWrite();

	int connectReadNormal();
	int disConnectReadNormal();
	int connectWriteNormal();
	int disConnectWriteNormal();

	int connectReadLocation();
	int disConnectReadLocation();
	int connectWriteLocation();
	int disConnectWriteLocation();

	int setEntityToEntityID(const char* entity,int entityID);
	int getEntityToEntityID(const char* entity,int& entityID);

	int setPredictToPreID(const char* predict,int preID);
	int getPredictToPreID(const char* predict,int& preID);

	int setLocationToLocID(const char* location,int locID);
	int getLocationToLocID(const char* location,int& locID);

	int setEntityIDToEntity(int entityID,const char* entity);
	int getEntityIDToEntity(int entityID,char* entity);

	int setPreIDToPredict(int preID,const char* predict);
	int getPreIDToPredict(int preID,char*& predict);

	int setLocIDToLocation(int locID,const char* location);
	int getLocIDToLocation(int locID,char*& location);

	int setLocIDToLocXLocY(int locID,double locX,double locY);
	int getLocIDToLocXLocY(int locID,double& locX,double& locY);

	int setLocXLocYToLocID(double locX,double locY,int locID);
	int setLocXLocYToLocIDList(double locX,double locY,int locIDListLen,int* locIDList);
	int getLocXLocYToLocIDList(double locXL,double locXH,double locYL,double locYH,int& locIDListLen,int*& locIDList);

	int setSubIDToLineID(int subID,int lineID);
	int setSubIDToLineIDList(int subID,int lineIDListLen,int* lineIDList);
	int getSubIDToLineIDList(int subID,int& lineIDListLen,int*& lineIDList);

	int setSubIDToObjID(int subID,int objID);
	int setSubIDToObjIDList(int subID,int objIDListLen,int* objIDList);
	int getSubIDToObjIDList(int subID,int& objIDListLen,int*& objIDList);

	int setSubIDPreIDToLineID(int subID,int preID,int lineID);
	int setSubIDPreIDToLineIDList(int subID,int preID,int lineIDListLen,int* lineIDList);
	int getSubIDPreIDToLineIDList(int subID,int preID,int& lineIDListLen,int*& lineIDList);

	int setSubIDPreIDToObjID(int subID,int preID,int objID);
	int setSubIDPreIDToObjIDList(int subID,int preID,int objIDListLen,int* objIDList);
	int getSubIDPreIDToObjIDList(int subID,int preID,int& objIDListLen,int*& objIDList);

	int setSubIDPreIDToLocID(int subID,int preID,int locID);
	int setSubIDPreIDToLocIDList(int subID,int preID,int locIDListLen,int* locIDList);
	int getSubIDPreIDToLocIDList(int subID,int preID,int& locIDListLen,int*& locIDList);

	int setObjIDToLineID(int objID,int lineID);
	int setObjIDToLineIDList(int objID,int lineIDListLen,int* lineIDList);
	int getObjIDToLineIDList(int objID,int& lineIDListLen,int*& lineIDList);

	int setObjIDToSubID(int objID,int subID);
	int setObjIDToSubIDList(int objID,int subIDListLen,int* subIDList);
	int getObjIDToSubIDList(int objID,int& subIDListLen,int*& subIDList);

	int setObjIDPreIDToLineID(int objID,int preID,int lineID);
	int setObjIDPreIDToLineIDList(int objID,int preID,int lineIDListLen,int* lineIDList);
	int getObjIDPreIDToLineIDList(int objID,int preID,int& lineIDListLen,int*& lineIDList);

	int setObjIDPreIDToSubID(int objID,int preID,int subID);
	int setObjIDPreIDToSubIDList(int objID,int preID,int subIDListLen,int* subIDList);
	int getObjIDPreIDToSubIDList(int objID,int preID,int& subIDListLen,int*& subIDList);

	int setObjIDPreIDToLocID(int objID,int preID,int locID);
	int setObjIDPreIDToLocIDList(int objID,int preID,int locIDListLen,int* locIDList);
	int getObjIDPreIDToLocIDList(int objID,int preID,int& locIDListLen,int*& locIDList);

	/*
	int setLocXLocYPreIDToLineID(double locX,double locY,int preID,int lineID);
	int setLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int lineIDListLen,int* lineIDList);
	int getLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int& lineIDListLen,int*& lineIDList);
	*/
	int setLineIDToSubID(int lineID,int subID);
	int getLineIDToSubID(int lineID,int& subID);

	int setLineIDToObjID(int lineID,int subID);
	int getLineIDToObjID(int lineID,int& subID);

	int setLineIDToPreID(int lineID,int subID);
	int getLineIDToPreID(int lineID,int& subID);

	int setLineIDToLocID(int lineID,int subID);
	int getLineIDToLocID(int lineID,int& subID);

	int setLineIDToSPOL(int lineID,int subID,int preID,int objID,int locID);
	int getLineIDToSPOL(int lineID,int& subID,int& preID,int& objID,int& locID);};

#endif /* DATABASEHBASE_H_ */
