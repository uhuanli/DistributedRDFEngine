/*
 * Sig.h
 *
 *  Created on: 2013-4-3
 *      Author: liyouhuan
 */

#ifndef TYPE_H_
#define TYPE_H_
#include<bitset>
#include<iostream>
#include<sstream>
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include "DEFINE.h"
#include<sys/time.h>
#include<map>
#include<vector>
#include<queue>
#include<set>
#include<time.h>
using namespace std;
class Sig{
public:
	int id;
	std::bitset<SIGNATURE_LENGTH> signature;
	Sig();
	std::string toString();
	void Print();
};
class hEdge{
public:
	Sig linkedNode;
	bitset<SIGNATURE_LENGTH> eSig;
	hEdge();
	hEdge(std::string _edge_line);
	hEdge(int _literalID, const bitset<SIGNATURE_LENGTH>& _e_sig);
	std::string toStringLine();
	void addStringLine(stringstream & _buf)const;
};
class string_triple
{
public:
	std::string subject;
	std::string predict;
	std::string object;
	std::string location;

	bool operator <(const string_triple & other) const;
	bool operator==(const string_triple & other) const;
	std::string toString() const;
};
class hResult{
public:
	int result[MAX_QUERY_MAP];
	hResult(const hResult& _hr){
		memcpy(result, _hr.result, sizeof(result));
	}
	hResult(int _v1, int _v2, int _v1ID, int _v2ID){
		Initial();
		result[_v1] = _v1ID;
		result[_v2] = _v2ID;
	}
	hResult(const hResult &_hr, int _except_v, int _except_v_ID){
		for(int i = 0; i < MAX_QUERY_MAP; i ++)
		{
			if(i == _except_v)
			{
				this ->result[i] = _except_v_ID;//
				continue;
			}
			this ->result[i] = _hr.result[i];
		}
	}
	bool invalid(int _var_num){
		for(int i = 0; i < _var_num; i ++){
			if(result[i] == -2 || result[i] == -1) return true;
		}
		return false;
	}
	std::string toString(int varNum)const;

	void Initial(){
		for(int i = 0; i < MAX_QUERY_MAP; i ++)
		{
			result[i] = -2;
		}
	}
};
class Interval{
	struct timeval _begin, _end;
	double _total_consume;
public:
	void begin(){
		gettimeofday(&_begin, NULL);
	}
	void end(){
		gettimeofday(&_end, NULL);
		_total_consume += (_end.tv_sec - _begin.tv_sec)*1000000 + (_end.tv_usec - _begin.tv_usec);
	}

	double curConsume(){
		timeval _cur;
		gettimeofday(&_cur, NULL);
		double _ret = (_cur.tv_sec - _begin.tv_sec)*1000000 + (_cur.tv_usec - _begin.tv_usec);
		return _ret;
	}

	Interval(){
		Initial();
	}
	void Log(std::string _log);
	void Initial(){
		_total_consume = 0.0;
	}
	double getConsumeTime(){
		return _total_consume / 1000000.0;
	}
};
class Runtime{
public:
	static Interval loadTreeTime;
	static Interval loadEdgeTime;
	static Interval loadCandidateEdge;
	static Interval totalRunTime;
	static Interval BFSTime;
	static Interval ShuffleTime;
	static Interval LiteralFilterTime;
	static Interval RetrieveTime;
	static Interval EncodeQueryTime;

	static void loadTreeBegin();
	static void loadTreeEnd();
	static void loadEdgeBegin();
	static void loadEdgeEnd();
	static void totalRunBegin();
	static void totalRunEnd();
	static void totalRunCheckPoint(std::string _log);

	static void loadCandidateEdgeBegin();
	static void loadCandidateEdgeEnd();

	static void Initial();
	static std::string toString();
	static void TimeLog();
//	static time_t JoinTime;
	static void EncodeBegin();
	static void EncodeEnd();
	static void RetrieveBegin();
	static void RetrieveEnd();
	static void LiteralFilterBegin();
	static void LiteralFilterEnd();
	static void ShuffleBegin();
	static void ShuffleEnd();
	static void BFS_Begin();
	static void BFS_End();

};
class hNode;
class hNodeData;

class hNodeData{//52
public:
	hNodeData();
	~hNodeData();
	hNode * pSon;//8
	Sig sig;//44
	std::string toString()const;
};

class hNode{//80 + 12 + 8 = 100
private:
	int level;
	int father;// -1 for the root's
	int hdataSize;
	hNodeData** hdata;
	Sig sig; // 4 + 40
	void setSize(int _sz);

public:
	std::vector<hEdge> outEdges;//12
	std::vector<hEdge> inEdges;//12
	std::vector<hEdge> literalEdges;//12

	hNode(Sig _sig, int _level, int _father);
	hNode(std::string _encoded_node);
	hNode(stringstream & _buf);
	int getID()const;
	int getSize()const;
	int getLevel()const;
	int getFather()const;
	int getInEdgeSize()const;
	int getOutEdgeSize()const;
	int getLiteralEdgeSize()const;

	void addOutEdge(std::string _edge);
	void addInEdge(std::string _edge);
	void loadData();
	void loadOutEdges();
	void loadInEdges();
	void loadSumEdges();
	void loadEdges();
	void loadRootedEdges();
	void loadRootedTree();
	bool isLeaf()const;
	bool isLeafElement()const;

	std::string encode()const;
	void resolveNodeFromBuf(stringstream& _buf);
	void addStringNode(stringstream& _buf)const;
	bool matchInV(const bitset<SIGNATURE_LENGTH> &_inV) const;
	bool matchOutV(const bitset<SIGNATURE_LENGTH> &_outV) const;
	//sig of outedge should covers _e_sig
	bool hasOutEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _toID)const;
	//sig of inedge should covers _e_sig
	bool hasInEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _fromID)const;
	//for literal filter
	bool hasLiteralEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _literalID)const;
	bool isCandidate(const bitset<SIGNATURE_LENGTH> &_bs) const;
	void getSonCandidates(std::vector<hNode*> & _sc_vec, const bitset<SIGNATURE_LENGTH> &_bs);
	std::string getBitString()const;

	std::string toString()const;
	std::string toEdgeString()const;
	void Print()const;
	void PrintEdges()const;
	void PrintRootedTree(std::string _pre)const;
	void PrintRootedEdges()const;
};

#endif /* TYPE_H_ */
