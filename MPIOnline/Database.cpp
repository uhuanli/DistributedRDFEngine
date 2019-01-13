//#include "Global.h"
#include "Database.h"


int Database::connectRead(){
	return 0;
}
int Database::disConnectRead(){
	return 0;
}
int Database::connectWrite(){
	return 0;
}
int Database::disConnectWrite(){
	return 0;
}


int Database::setEntityToEntityID(const char* entity,int  entityID){
	return 0;
}
int Database::getEntityToEntityID(const char* entity,int& entityID){
	return 0;
}

int Database::setPredictToPreID(const char* predict,int  preID){
	return 0;
}
int Database::getPredictToPreID(const char* predict,int& preID){
	return 0;
}

int Database::setLocationToLocID(const char* location,int locID){
	return 0;
}
int Database::getLocationToLocID(const char* location,int& locID){
	return 0;
}

int Database::setEntityIDToEntity(int entityID,const char* entity){
	return 0;
}
int Database::getEntityIDToEntity(int entityID,char*& entity){
	return 0;
}

int Database::setPreIDToPredict(int preID,const char* predict){
	return 0;
}
int Database::getPreIDToPredict(int preID,char*& predict){
	return 0;
}

int Database::setLocIDToLocation(int locID,const char* location){
	return 0;
}
int Database::getLocIDToLocation(int locID,char*& location){
	return 0;
}


int Database::setLocIDToLocXLocY(int locID,double locX,double locY){
	return 0;
}
int Database::getLocIDToLocXLocY(int locID,double& locX,double& locY){
	return 0;
}

int Database::setLocXLocYToLocID(double locX,double locY,int locID){
	return 0;
}
int Database::setLocXLocYToLocIDList(double locX,double locY,int locIDListLen,int* locIDList){
	return 0;
}
int Database::getLocXLocYToLocIDList(double locXL,double locXH,double locYL,double locYH,int& locIDListLen,int*& locIDList){
	return 0;
}

int Database::setSubIDToLineID(int subID,int lineID){
	return 0;
}
int Database::setSubIDToLineIDList(int subID,int lineIDListLen,int* lineIDList){
	return 0;
}
int Database::getSubIDToLineIDList(int subID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int Database::setSubIDToObjID(int subID,int objID){
	return 0;
}
int Database::setSubIDToObjIDList(int subID,int objIDListLen,int* objIDList){
	return 0;
}
int Database::getSubIDToObjIDList(int subID,int& objIDListLen,int*& objIDList){
	return 0;
}

int Database::setSubIDPreIDToLineID(int subID,int preID,int lineID){
	return 0;
};
int Database::setSubIDPreIDToLineIDList(int subID,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int Database::getSubIDPreIDToLineIDList(int subID,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int Database::setSubIDPreIDToObjID(int subID,int preID,int objID){
	return 0;
}
int Database::setSubIDPreIDToObjIDList(int subID,int preID,int objIDListLen,int* objIDList){
	return 0;
}
int Database::getSubIDPreIDToObjIDList(int subID,int preID,int& objIDListLen,int*& objIDList){
	return 0;
}

int Database::setSubIDPreIDToLocID(int subID,int preID,int locID){
	return 0;
};
int Database::setSubIDPreIDToLocIDList(int subID,int preID,int locIDListLen,int* locIDList){
	return 0;
};
int Database::getSubIDPreIDToLocIDList(int subID,int preID,int& locIDListLen,int*& locIDList){
	return 0;
};


int Database::setObjIDToLineID(int objID,int lineID){
	return 0;
}
int Database::setObjIDToLineIDList(int objID,int lineIDListLen,int* lineIDList){
	return 0;
}
int Database::getObjIDToLineIDList(int objID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int Database::setObjIDToSubID(int objID,int subID){
	return 0;
}
int Database::setObjIDToSubIDList(int objID,int subIDListLen,int* subIDList){
	return 0;
}
int Database::getObjIDToSubIDList(int objID,int& subIDListLen,int*& subIDList){
	return 0;
}

int Database::setObjIDPreIDToLineID(int objID,int preID,int lineID){
	return 0;
}
int Database::setObjIDPreIDToLineIDList(int objID,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int Database::getObjIDPreIDToLineIDList(int objID,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int Database::setObjIDPreIDToSubID(int objID,int preID,int subID){
	return 0;
}
int Database::setObjIDPreIDToSubIDList(int objID,int preID,int subIDListLen,int* subIDList){
	return 0;
}
int Database::getObjIDPreIDToSubIDList(int objID,int preID,int& subIDListLen,int*& subIDList){
	return 0;
}


int Database::setObjIDPreIDToLocID(int objID,int preID,int locID){
	return 0;
}
int Database::setObjIDPreIDToLocIDList(int objID,int preID,int locIDListLen,int* locIDList){
	return 0;
}
int Database::getObjIDPreIDToLocIDList(int objID,int preID,int& locIDListLen,int*& locIDList){
	return 0;
}


/*
int Database::setLocXLocYPreIDToLineID(double locX,double locY,int preID,int lineID){
	return 0;
}
int Database::setLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int Database::getLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}
*/
int Database::setLineIDToSubID(int lineID,int subID){
	return 0;
}
int Database::getLineIDToSubID(int lineID,int& subID){
	return 0;
}

int Database::setLineIDToObjID(int lineID,int objID){
	return 0;
}
int Database::getLineIDToObjID(int lineID,int& objID){
	return 0;
}

int Database::setLineIDToPreID(int lineID,int preID){
	return 0;
}
int Database::getLineIDToPreID(int lineID,int& preID){
	return 0;
}

int Database::setLineIDToLocID(int lineID,int locID){
	return 0;
}
int Database::getLineIDToLocID(int lineID,int& locID){
	return 0;
}

int Database::setLineIDToSPOL(int lineID,int subID,int preID,int objID,int locID){
	return 0;
}
int Database::getLineIDToSPOL(int lineID,int& subID,int& preID,int& objID,int& locID){
	return 0;
}
