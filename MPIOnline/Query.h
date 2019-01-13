/*
 * Query.h
 *
 *  Created on: 2013-4-7
 *      Author: liyouhuan
 */

#ifndef QUERY_H_
#define QUERY_H_
#include "Type.h"
#include "DEFINE.h"
class Query{
public:
	int queryEdgeNum;
	int queryVertexNum;
	int queryVertexEdgeNum[MAX_QUERY_MAP];
	int queryVertexEdgeID[MAX_QUERY_MAP][MAX_QUERY_MAP];
	int queryVertexEdgeType[MAX_QUERY_MAP][MAX_QUERY_MAP];
	std::map<int,string_triple> queryStringTriples;
	std::string queryVertexName[MAX_QUERY_MAP];
	std::string queryStream;
	bitset<SIGNATURE_LENGTH> queryVertexSignatures[MAX_QUERY_MAP];
	std::map<std::string, int> varStr2ID;
	std::vector<hNode*> candidateVec[MAX_QUERY_MAP];
	bitset<SIGNATURE_LENGTH> edgePredict[MAX_QUERY_MAP][MAX_QUERY_MAP];
	bool isEdge[MAX_QUERY_MAP][MAX_QUERY_MAP];
	int varsDistribution [MAX_COMPUTE_NODE][MAX_QUERY_MAP];
	int visitStatus[MAX_QUERY_MAP];
	//one column is one set of result
	std::vector<hResult> resultSet;
	std::vector<int> isolatedVertex[MAX_QUERY_MAP];
	bool isResultSetEmpty[MAX_QUERY_MAP];
	bool isNeverJoin;

	Query();
	~Query();

	std::string getQueryStream();
	int getVertexNum();
	bitset<SIGNATURE_LENGTH> getVertexSignature(int _i);
	void addCandidates(int _i, hNode * _pNode);
	std::vector<hNode*> & getCandidateVec(int _i);
	void candidateInit(const std::vector<hNode*> & _root_vec);
	void clearCandidate();
	void getIntoNextLevel();
	void getVarsInThisNode(int * _vars);
	void neighbourFilter();
	void outFilter(int _i, const bitset<SIGNATURE_LENGTH>& outSig);
	void inFilter(int _i, const bitset<SIGNATURE_LENGTH>& inSig);

	void shuffleCandidate(int _cur_rank, int _into_rank, int _varID, int _size);
	void getShuffleString(stringstream & _buf, int _varID);
	void resolveShuffleCandidate(stringstream & _buf, int _varID);
	//just as prePreEvaluate()
	void filteredByLiteralEdge();
	int get_BFS_start_Root();
	void addMiddleResultSet(int _v1, int _v2, std::multimap<int, int>& v1_To_v2);

	void master_Join(int _v1, int _v2, const std::string& _join_result);
	void master_Add_IsolatedVertex(int _v, const std::string& _isolated_IDs);
	void one_step_Join(int _v1, int _v2,
			const std::string &_IDs_v1_to_v2, std::string& _join_result);
	//both _v1 and _v2 could be fromV and toV BUT only _v1 sent to _v2!!!
	void dealWithEdge(int _r, int _v1, int v2);
	void dealWithIsolatedVertex(int _r, int _v);
	std::string getCandidateIDs(int _i);
	void BFS_VisitQueryGraph(int _r, int _s);
	bool AllVisited();
	bool hasEdge(int _v);
	int findUnvisited();
	void PrintResult(int _r);
	void loadCandidateEdge();

	void setQueryStream(std::string _query_stream);
	std::string toString();
	void getStringTriple(string_triple &_triple, const char* queryLine);
	void buildQueryStringTriples();
	void generateSignature();
	void Print();
	void qLog();
};

#endif /* QUERY_H_ */
