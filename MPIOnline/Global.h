/*
 * Global.h
 *
 *  Created on: 2013-4-1
 *      Author: liyouhuan
 */

#ifndef GLOBAL_H_
#define GLOBAL_H_
#include <mpi.h>
#include "HBASE.h"
#include "Type.h"
#include "STREE.h"
#include "DatabaseHBase.h"
#include "Query.h"

using namespace std;
//class DatabaseHBase;
//class sTree;

class Global{
public:
	static int Initial(char processor_name[]);
	static int Finalize(char processor_name[]);
	static void Log(const char _str_log[]);
	static void Log(const char _str_log[], const std::string _tab);
	static void Log_(const char _str_log[], const std::string _tab);
	static void Print(const char _str_log[]);
	static int getBitset (std::bitset<SIGNATURE_LENGTH> & _sig, std::string _str_sig);
	static int long_toBitset(std::bitset<SIGNATURE_LENGTH> & _sig, long long _l[]);
	static int bitset_toLong(const bitset<SIGNATURE_LENGTH> & _sig, long long _l[]);
	static int getSignature(Sig & _sig, std::string _str_signature);
	static void inputQueryStream(std::string &_query_string);// "end" included
	//
	static unsigned int BKDRHash(char *str);
	static void buildEntitySignature(const std::string& stringlabel,
							bitset<SIGNATURE_LENGTH_O + SIGNATURE_LENGTH_S> & sigcont,
							const int offset);
	static void buildPredictSignature(const int preID,
							bitset<SIGNATURE_LENGTH_O + SIGNATURE_LENGTH_S> & sigcont ,
							const int offset);
	static void buildPredictSignatureO(const int preID, bitset<SIGNATURE_LENGTH_O> & sigcont ,const int offset);

	static void Retrieve(Query& _q);
	static void loadCandidateEdge(Query& _q);
	//
	static void loadStree(int _r, int _s);
	static void loadEdges(int _r, int _s);
	static void QueryInput(int _r, int _s, Query & _q);
	static void Shuffle(int _r, int _s, Query& myQuery);
	static void shareVarsDistribution(int _r, int _s, Query & _q);
	static int getRankByVarID(int _varID);
	static int getVarIDByRank(int _rank);
	static sTree* getStree();

	static DatabaseHBase* myDatabase;
	static sTree* Stree;
	static FILE * log;
	static bool testTopLevel;
	static bool testShuffle;
	static bool test_a1_loading;
	static bool test_a2_loading;
	static bool load_NO_LEAF_edges;
	static bool test_c1_loading;
	static bool load_edge;
	static bool debug_mode;
	static bool test_sumEdge;
	static bool test_loadTree;
	static bool runQuery;
	static bool loadsumEdge;
	static bool logRetrieve;

	static int subNumber;
	static int locationEnable;
	static const int SUBLEN;
	static int hashType;
	static char recvBuffer[50*1000*1000];

	static std::string streeTable;
	static std::string E2ETable;
	static std::string V2ETable;

	static std::string sub2IDTable;
	static std::string pre2IDTable;
	static std::string obj2IDTable;
	static std::string ID2subTable;
	static std::string ID2preTable;
	static std::string ID2objTable;
	static std::string SumEdgeTable;

	static std::string strCol;
	static std::string idCol;

	static std::string InSizeCol[10];
	static std::string OutSizeCol[10];
	static std::string LiteralSizeCol[10];
	static std::string ftCol[10];
	static std::string sgCol[10];
	static std::string szCol[10];
	static std::string eSigCol[10];
	static std::string nieCol[10];
	static std::string noeCol[10];
	static std::string fieCol[10];
	static std::string foeCol[10];

	static std::string sons[MAX_SON_NUMBER];
};

#endif /* GLOBAL_H_ */
