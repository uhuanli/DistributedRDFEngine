#ifndef DATABASE_H
#define DATABASE_H
//#include "Global.h"

class Database{
private:
	int sqlType;
public:
	virtual int connectRead();
	virtual int disConnectRead();
	virtual int connectWrite();
	virtual int disConnectWrite();

	virtual int setEntityToEntityID(const char* entity,int entityID);
	virtual int getEntityToEntityID(const char* entity,int& entityID);

	virtual int setPredictToPreID(const char* predict,int preID);
	virtual int getPredictToPreID(const char* predict,int& preID);

	virtual int setLocationToLocID(const char* location,int locID);
	virtual int getLocationToLocID(const char* location,int& locID);

	virtual int setEntityIDToEntity(int entityID,const char* entity);
	virtual int getEntityIDToEntity(int entityID,char*& entity);

	virtual int setPreIDToPredict(int preID,const char* predict);
	virtual int getPreIDToPredict(int preID,char*& predict);

	virtual int setLocIDToLocation(int locID,const char* location);
	virtual int getLocIDToLocation(int locID,char*& location);

	virtual int setLocIDToLocXLocY(int locID,double locX,double locY);
	virtual int getLocIDToLocXLocY(int locID,double& locX,double& locY);

	virtual int setLocXLocYToLocID(double locX,double locY,int locID);
	virtual int setLocXLocYToLocIDList(double locX,double locY,int locIDListLen,int* locIDList);
	virtual int getLocXLocYToLocIDList(double locXL,double locXH,double locYL,double locYH,int& locIDListLen,int*& locIDList);

	virtual int setSubIDToLineID(int subID,int lineID);
	virtual int setSubIDToLineIDList(int subID,int lineIDListLen,int* lineIDList);
	virtual int getSubIDToLineIDList(int subID,int& lineIDListLen,int*& lineIDList);

	virtual int setSubIDToObjID(int subID,int objID);
	virtual int setSubIDToObjIDList(int subID,int objIDListLen,int* objIDList);
	virtual int getSubIDToObjIDList(int subID,int& objIDListLen,int*& objIDList);

	virtual int setSubIDPreIDToLineID(int subID,int preID,int lineID);
	virtual int setSubIDPreIDToLineIDList(int subID,int preID,int lineIDListLen,int* lineIDList);
	virtual int getSubIDPreIDToLineIDList(int subID,int preID,int& lineIDListLen,int*& lineIDList);

	virtual int setSubIDPreIDToObjID(int subID,int preID,int objID);
	virtual int setSubIDPreIDToObjIDList(int subID,int preID,int objIDListLen,int* objIDList);
	virtual int getSubIDPreIDToObjIDList(int subID,int preID,int& objIDListLen,int*& objIDList);

	virtual int setSubIDPreIDToLocID(int subID,int preID,int locID);
	virtual int setSubIDPreIDToLocIDList(int subID,int preID,int locIDListLen,int* locIDList);
	virtual int getSubIDPreIDToLocIDList(int subID,int preID,int& locIDListLen,int*& locIDList);

	virtual int setObjIDToLineID(int objID,int lineID);
	virtual int setObjIDToLineIDList(int objID,int lineIDListLen,int* lineIDList);
	virtual int getObjIDToLineIDList(int objID,int& lineIDListLen,int*& lineIDList);

	virtual int setObjIDToSubID(int objID,int subID);
	virtual int setObjIDToSubIDList(int objID,int subIDListLen,int* subIDList);
	virtual int getObjIDToSubIDList(int objID,int& subIDListLen,int*& subIDList);

	virtual int setObjIDPreIDToLineID(int objID,int preID,int lineID);
	virtual int setObjIDPreIDToLineIDList(int objID,int preID,int lineIDListLen,int* lineIDList);
	virtual int getObjIDPreIDToLineIDList(int objID,int preID,int& lineIDListLen,int*& lineIDList);

	virtual int setObjIDPreIDToSubID(int objID,int preID,int subID);
	virtual int setObjIDPreIDToSubIDList(int objID,int preID,int subIDListLen,int* subIDList);
	virtual int getObjIDPreIDToSubIDList(int objID,int preID,int& subIDListLen,int*& subIDList);

	virtual int setObjIDPreIDToLocID(int objID,int preID,int locID);
	virtual int setObjIDPreIDToLocIDList(int objID,int preID,int locIDListLen,int* locIDList);
	virtual int getObjIDPreIDToLocIDList(int objID,int preID,int& locIDListLen,int*& locIDList);

	/*
	virtual int setLocXLocYPreIDToLineID(double locX,double locY,int preID,int lineID);
	virtual int setLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int lineIDListLen,int* lineIDList);
	virtual int getLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int& lineIDListLen,int*& lineIDList);
	*/

	virtual int setLineIDToSubID(int lineID,int subID);
	virtual int getLineIDToSubID(int lineID,int& subID);

	virtual int setLineIDToObjID(int lineID,int subID);
	virtual int getLineIDToObjID(int lineID,int& subID);

	virtual int setLineIDToPreID(int lineID,int subID);
	virtual int getLineIDToPreID(int lineID,int& subID);

	virtual int setLineIDToLocID(int lineID,int subID);
	virtual int getLineIDToLocID(int lineID,int& subID);

	virtual int setLineIDToSPOL(int lineID,int subID,int preID,int objID,int locID);
	virtual int getLineIDToSPOL(int lineID,int& subID,int& preID,int& objID,int& locID);

};
#endif

