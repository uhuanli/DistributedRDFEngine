/*
 * DatabaseHBase.cpp
 *
 *  Created on: 2013-3-31
 *      Author: liyouhuan
 */
//#include "Global.h"
#include "Database.h"
#include "DatabaseHBase.h"
#include "HBASE.h"
#include "Global.h"
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<iostream>
using namespace std;
//@liyouhuan

bool DatabaseHBase::loadPreID(){
	Global::Log("IN loadPreID", "***");
	pre2ID.clear();
	ID2pre.clear();

	HbaseClient* client = HBASE::getClient();
	std::vector<std::string> columnNames;
	columnNames.push_back(Global::strCol);
	int scanner = client ->scannerOpen(Global::ID2preTable, "", columnNames);
	try{
		while (true)
		{
			std::vector<TRowResult> value;
			client ->scannerGet(value, scanner);
			{
				if (value.size() == 0){
					Global::Log("loadPreID: value.size = 0");
					break;
				}
			}
			std::string _id = value[value.size() - 1].row;
			int preID = 0;
			{
				stringstream _tmp_ss(_id.c_str());
				_tmp_ss >> preID;
			}
			std::map<std::string, TCell> s2Cell = value[value.size() - 1].columns;
			std::string _pre = s2Cell[Global::strCol].value;
			ID2pre.insert(pair<int, std::string>(preID, _pre));
			pre2ID.insert(pair<std::string, int>(_pre, preID));
			Global::Log((_id + "=" + _pre).c_str());
		}
	}
	catch (const IOError &ioe)
	{
		  std::cout << "FATAL: Scanner raised IOError" << std::endl;
		  Global::Log("FATAL: Scanner raised IOError");
	}
	client ->scannerClose(scanner);

	Global::Log("OUT loadPreID", "***");
	return true;
}
int DatabaseHBase::getStrByID(char* _str, int _id, std::string _table)
{
	char temp[100];
	sprintf(temp, "%d", _id);
	std::string _ID = std::string(temp);
	std::string _val;
	HBASE::getValue(_val, _table, _ID, Global::strCol);
	if(_val == ""){
		_str = NULL;
		return -1;
	}
//	_str = new char[_val.length() + 1];
	strcpy(_str, _val.c_str());
	return 0;
}
int DatabaseHBase::getIDByStr(const char* _str, int &_id, std::string _table)
{
	std::string _Str = string(_str);
	std::string _val = "";
	HBASE::getValue(_val, _table, _Str, Global::idCol);
	if(_val == ""){
		_id = -1;
		return -1;
	}
	_id = atoi(_val.c_str());
	return 0;
}
int DatabaseHBase::getSubByID(char* _sub, int _subID){
	return getStrByID(_sub, _subID, Global::ID2subTable);
}
int DatabaseHBase::getObjByID(char*& _obj, int _objID){
	return getStrByID(_obj, _objID, Global::ID2objTable);
}
int DatabaseHBase::getIDBySub(const char* _sub, int &_subID){
	return getIDByStr(_sub, _subID, Global::sub2IDTable);
}
int DatabaseHBase::getIDByObj(const char* _obj, int &_objID){
	return getIDByStr(_obj, _objID, Global::obj2IDTable);
}
int DatabaseHBase::connect(){
	HBASE::connect();
	DatabaseHBase::loadPreID();
	return 0;
}
int DatabaseHBase::disconnect(){
	HBASE::disconnect();
	return 0;
}
//@vstree_yago
int DatabaseHBase::connectRead(){
	HBASE::connect();
	return 0;
}
int DatabaseHBase::disConnectRead(){
	HBASE::disconnect();
	return 0;
}
int DatabaseHBase::connectWrite(){
//	HBASE::connect(); need to be modified!!
	return 0;
}
int DatabaseHBase::disConnectWrite(){
//	HBASE::disconnect();
	return 0;
}


int DatabaseHBase::setEntityToEntityID(const char* entity,int  entityID){
	return 0;
}
int DatabaseHBase::getEntityToEntityID(const char* entity,int& entityID){
	this ->getIDBySub(entity, entityID);
	if(entityID == -1){
		this ->getIDByObj(entity, entityID);
	}
	return 0;
}

int DatabaseHBase::setPredictToPreID(const char* predict,int  preID){
	return 0;
}
int DatabaseHBase::getPredictToPreID(const char* predict,int& preID){
	if(! pre2ID.empty())
	{
		preID = pre2ID[std::string(predict)];
	}
	this ->getIDByStr(predict, preID, Global::pre2IDTable);
	return 0;
}

int DatabaseHBase::setLocationToLocID(const char* location,int locID){
	return 0;
}
int DatabaseHBase::getLocationToLocID(const char* location,int& locID){
	return 0;
}

int DatabaseHBase::setEntityIDToEntity(int entityID,const char* entity){
	return 0;
}
int DatabaseHBase::getEntityIDToEntity(int entityID,char* entity){
	this ->getSubByID(entity, entityID);
	return 0;
}

int DatabaseHBase::setPreIDToPredict(int preID,const char* predict){
	return 0;
}
int DatabaseHBase::getPreIDToPredict(int preID,char*& predict){
	return 0;
}

int DatabaseHBase::setLocIDToLocation(int locID,const char* location){
	return 0;
}
int DatabaseHBase::getLocIDToLocation(int locID,char*& location){
	return 0;
}


int DatabaseHBase::setLocIDToLocXLocY(int locID,double locX,double locY){
	return 0;
}
int DatabaseHBase::getLocIDToLocXLocY(int locID,double& locX,double& locY){
	return 0;
}

int DatabaseHBase::setLocXLocYToLocID(double locX,double locY,int locID){
	return 0;
}
int DatabaseHBase::setLocXLocYToLocIDList(double locX,double locY,int locIDListLen,int* locIDList){
	return 0;
}
int DatabaseHBase::getLocXLocYToLocIDList(double locXL,double locXH,double locYL,double locYH,int& locIDListLen,int*& locIDList){
	return 0;
}

int DatabaseHBase::setSubIDToLineID(int subID,int lineID){
	return 0;
}
int DatabaseHBase::setSubIDToLineIDList(int subID,int lineIDListLen,int* lineIDList){
	return 0;
}
int DatabaseHBase::getSubIDToLineIDList(int subID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int DatabaseHBase::setSubIDToObjID(int subID,int objID){
	return 0;
}
int DatabaseHBase::setSubIDToObjIDList(int subID,int objIDListLen,int* objIDList){
	return 0;
}
int DatabaseHBase::getSubIDToObjIDList(int subID,int& objIDListLen,int*& objIDList){
	return 0;
}

int DatabaseHBase::setSubIDPreIDToLineID(int subID,int preID,int lineID){
	return 0;
};
int DatabaseHBase::setSubIDPreIDToLineIDList(int subID,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int DatabaseHBase::getSubIDPreIDToLineIDList(int subID,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int DatabaseHBase::setSubIDPreIDToObjID(int subID,int preID,int objID){
	return 0;
}
int DatabaseHBase::setSubIDPreIDToObjIDList(int subID,int preID,int objIDListLen,int* objIDList){
	return 0;
}
int DatabaseHBase::getSubIDPreIDToObjIDList(int subID,int preID,int& objIDListLen,int*& objIDList){
	return 0;
}

int DatabaseHBase::setSubIDPreIDToLocID(int subID,int preID,int locID){
	return 0;
};
int DatabaseHBase::setSubIDPreIDToLocIDList(int subID,int preID,int locIDListLen,int* locIDList){
	return 0;
};
int DatabaseHBase::getSubIDPreIDToLocIDList(int subID,int preID,int& locIDListLen,int*& locIDList){
	return 0;
};


int DatabaseHBase::setObjIDToLineID(int objID,int lineID){
	return 0;
}
int DatabaseHBase::setObjIDToLineIDList(int objID,int lineIDListLen,int* lineIDList){
	return 0;
}
int DatabaseHBase::getObjIDToLineIDList(int objID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int DatabaseHBase::setObjIDToSubID(int objID,int subID){
	return 0;
}
int DatabaseHBase::setObjIDToSubIDList(int objID,int subIDListLen,int* subIDList){
	return 0;
}
int DatabaseHBase::getObjIDToSubIDList(int objID,int& subIDListLen,int*& subIDList){
	return 0;
}

int DatabaseHBase::setObjIDPreIDToLineID(int objID,int preID,int lineID){
	return 0;
}
int DatabaseHBase::setObjIDPreIDToLineIDList(int objID,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int DatabaseHBase::getObjIDPreIDToLineIDList(int objID,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}

int DatabaseHBase::setObjIDPreIDToSubID(int objID,int preID,int subID){
	return 0;
}
int DatabaseHBase::setObjIDPreIDToSubIDList(int objID,int preID,int subIDListLen,int* subIDList){
	return 0;
}
int DatabaseHBase::getObjIDPreIDToSubIDList(int objID,int preID,int& subIDListLen,int*& subIDList){
	return 0;
}


int DatabaseHBase::setObjIDPreIDToLocID(int objID,int preID,int locID){
	return 0;
}
int DatabaseHBase::setObjIDPreIDToLocIDList(int objID,int preID,int locIDListLen,int* locIDList){
	return 0;
}
int DatabaseHBase::getObjIDPreIDToLocIDList(int objID,int preID,int& locIDListLen,int*& locIDList){
	return 0;
}


/*
int DatabaseHBase::setLocXLocYPreIDToLineID(double locX,double locY,int preID,int lineID){
	return 0;
}
int DatabaseHBase::setLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int lineIDListLen,int* lineIDList){
	return 0;
}
int DatabaseHBase::getLocXLocYPreIDToLineIDList(double locXL,double locXH,double locYL,double locYH,int preID,int& lineIDListLen,int*& lineIDList){
	return 0;
}
*/
int DatabaseHBase::setLineIDToSubID(int lineID,int subID){
	return 0;
}
int DatabaseHBase::getLineIDToSubID(int lineID,int& subID){
	return 0;
}

int DatabaseHBase::setLineIDToObjID(int lineID,int objID){
	return 0;
}
int DatabaseHBase::getLineIDToObjID(int lineID,int& objID){
	return 0;
}

int DatabaseHBase::setLineIDToPreID(int lineID,int preID){
	return 0;
}
int DatabaseHBase::getLineIDToPreID(int lineID,int& preID){
	return 0;
}

int DatabaseHBase::setLineIDToLocID(int lineID,int locID){
	return 0;
}
int DatabaseHBase::getLineIDToLocID(int lineID,int& locID){
	return 0;
}

int DatabaseHBase::setLineIDToSPOL(int lineID,int subID,int preID,int objID,int locID){
	return 0;
}
int DatabaseHBase::getLineIDToSPOL(int lineID,int& subID,int& preID,int& objID,int& locID){
	return 0;
}

std::map<int, std::string> DatabaseHBase::ID2pre;
std::map<std::string, int> DatabaseHBase::pre2ID;

