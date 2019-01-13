/*
 * Global.cpp
 *
 *  Created on: 2013-4-1
 *      Author: liyouhuan
 */
#include "Global.h"
#include "DatabaseHBase.h"
#include "STREE.h"

int Global::Initial(char processor_name[]){
	char logFile[100] = "./logs/";
	strcat(logFile, processor_name);
	Global::log = fopen(logFile, "wt+");
	if(Global::log == NULL) return -1;
	Global::Log((std::string("Initial:") + std::string(processor_name)).c_str(), "");

	myDatabase = new DatabaseHBase();
	Stree = new sTree(3);
	locationEnable = 0;
	myDatabase ->connect();
	return 0;
}
int Global::Finalize(char processor_name[])
{
	Global::Log("In Finalize...", "");
	myDatabase ->disconnect();
	Global::Log("finish disconnect database", "");
	delete myDatabase;
	Global::Log("finish delete myDatabase", "");
	delete Stree;
	Global::Log("finish delete Stree", "");
	Global::Log((std::string("Finalize:") + std::string(processor_name)).c_str(), "");
	fclose(Global::log);
	printf("finish finalize...%s\n", processor_name);
	return 0;
}
void Global::Log(const char _str_log[]){
	fprintf(Global::log, "\t%s\n", _str_log);
	fflush(Global::log);
}
void Global::Log_(const char _str_log[], const std::string _tab){
	fprintf(Global::log, "%s %s", _str_log, _tab.c_str());
	fflush(Global::log);
}
void Global::Log(const char _str_log[], std::string _pre){
	fprintf(Global::log, "%s%s\n", _pre.c_str(), _str_log);
	fflush(Global::log);
}
int Global::long_toBitset(std::bitset<SIGNATURE_LENGTH> & _sig, long long _l[]){
	int _index_bit = 0;
//	Global::Log("In toBitset", "");
	for(int i = 0; i < 5; i ++)
	{
		for(int j = 0; j < LONG_BIT_NUMBER; j ++)
		{
			if((_l[i] & 1LL << j) != 0)
			{
				_sig.set(_index_bit);
			}
			_index_bit ++;
		}
	}
//	Global::Log("Out toBitset", "");
	return 0;
}
int Global::bitset_toLong(const bitset<SIGNATURE_LENGTH> & _sig, long long _l[]){
	int _index_bit = 0;
	for(int i = 0; i < SIG_LONG_NUM; i ++)
	{
		_l[i] &= 0LL;
		for(int j = 0; j < LONG_BIT_NUMBER; j ++)
		{
			if(_sig[_index_bit])
			{
				_l[i] |= (1LL << j);
			}
			_index_bit ++;
		}
	}
	return 0;
}
int Global::getBitset (std::bitset<SIGNATURE_LENGTH> & _sig, std::string _str_sig){
	long long _l[5];
	{
		stringstream _tmp_ss(_str_sig);
		for(int i = 0; i < 5; i ++) _tmp_ss >> _l[i];
	}
	//check _sig is cleared
	Global::long_toBitset(_sig, _l);
	return 0;
}
int Global::getSignature(Sig & _sig, std::string _str_signature){
//	Global::Log("In getSignature", "");
	long long _l[5] = {};
	{
		stringstream _tmp_ss(_str_signature);
		_tmp_ss >> _sig.id;
		for(int i = 0; i < 5; i ++) _tmp_ss >> _l[i];
	}
	Global::long_toBitset(_sig.signature, _l);
//	Global::Log("Out getSignature", "");
	return 0;
}

void Global::inputQueryStream(std::string &_query_string){
	char* newLine = new char[1000];
	printf("please input the query...\n");
	std::cin.getline(newLine, 999);
	stringstream _buf;
	while(true){
		_buf << newLine << "\n";
		if(strcmp(newLine, "end") == 0) break;
		std::cin.getline(newLine, 999);
	}
	_query_string = _buf.str();
}

sTree* Global::getStree(){
	return Stree;
}
/*
 *
 */
unsigned int Global::BKDRHash(char *str)
{
	unsigned int seed = 131; // 31 131 1313 13131 131313 etc..
	unsigned int hash = 0;

	while (*str)
	{
		hash = hash * seed + (*str++);
	}

	return (hash & 0x7FFFFFFF);
}
void Global::buildEntitySignature(const std::string& stringlabel,  bitset<SIGNATURE_LENGTH_O + SIGNATURE_LENGTH_S> & sigcont,const int offset=0)
{// typeofHashFun: 1. BKDRHash hash function; 2
	if(stringlabel.length() >0 && stringlabel.at(0)=='?')
		return;
	int length = (int) stringlabel.length();
	if (hashType==0){
		if(length >=SUBLEN)
		{
			for(int i=0; i<=length-SUBLEN; i++)
			{
				string testsubstring = stringlabel.substr(i,SUBLEN);
				char substring[SUBLEN+1];
				memcpy(substring,testsubstring.c_str(),SUBLEN+1);
				unsigned int hashvalue = BKDRHash(substring);
				unsigned int pos = (hashvalue+offset) % (SIGNATURE_LENGTH_S);
				sigcont.set(pos);
			}
		}
	}
	else if (hashType==1){
		const int seedNum=10;
		int seed[seedNum]={2,3,5,7,11,13,17,19,23,29};
		unsigned int pos=0;
		for (int j=0;j<seedNum;j++){
			pos=0;
			for(int i=0; i<=length; i++){
				pos=(pos*seed[j]+stringlabel.at(0)+offset) % SIGNATURE_LENGTH_S;
			}
			sigcont.set(pos);
			//cout<<"pos "<<j<<": "<<pos<<endl;
		}
	}
}
//offset of outEdge is 1 and inEdge's is 0
void Global::buildPredictSignature(const int preID, bitset<SIGNATURE_LENGTH_O + SIGNATURE_LENGTH_S> & sigcont ,const int offset=0){
	if(preID < 0) return;
	int seed=3;
	int pos=(preID+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S;
	int pos2=(preID*preID+preID*seed+offset)%SIGNATURE_LENGTH_O+SIGNATURE_LENGTH_S;
	sigcont.set(pos);
	sigcont.set(pos2);
	return;
}
void Global::buildPredictSignatureO(const int preID, bitset<SIGNATURE_LENGTH_O> & sigcont ,const int offset=0){
	int seed=3;
	int pos=(preID+offset)%SIGNATURE_LENGTH_O;
	int pos2=(preID*preID+preID*seed+offset)%SIGNATURE_LENGTH_O;
	sigcont.set(pos);
	sigcont.set(pos2);
	return;
}
void Global::loadStree(int _r, int _s){
	Global::Log("IN loadStree");
	{
		Runtime::loadTreeBegin();
		if(_r == 0){
			Global::Log("this is rank, no loading tree...");
			Runtime::loadTreeEnd();
			return;
		}
	}
	Global::Log("loading tree...", "");
	Global::Stree ->loadStree(_r, _s, Global::Stree ->getRootLevel());
	Global::Log("finish loading tree...", "");
	stringstream _ss;
	_ss << "finish loading tree: " << _r;
	Global::Print(_ss.str().c_str());
	{
		Runtime::loadTreeEnd();
		Runtime::loadTreeTime.Log("loadTree time: ");
	}
	Global::Log("OUT loadStree");
}
void Global::loadEdges(int _r, int _s){
	Global::Log("IN Global::loadEdge");
	{
		Runtime::loadEdgeBegin();
	}
	Global::Log("loading edges...", "");
	printf("loading edges... on rank %d\n", _r);
	Global::Stree ->loadTreeEdges(_r, _s);
	Global::Log("finish loading edges...", "");
	printf("finish loading edges... on rank %d\n", _r);
	stringstream _ss;
	_ss << "finish loading edges: " << _r;
	Global::Print(_ss.str().c_str());
	{
		Runtime::loadEdgeEnd();
		Runtime::loadEdgeTime.Log("loadEdge: ");
	}
	Global::Log("OUT Global::loadEdge");
}
void Global::Print(const char _str_log[]){
	printf("%s\n", _str_log);
}
void Global::QueryInput(int _r, int _s, Query & _q){
	char _query[10000];
	memset(_query, 0, sizeof(_query));
	int _q_len = 0;

	if(_r == 0){
		std::string _q_str;
		Global::inputQueryStream(_q_str);
		strcpy(_query, _q_str.c_str());
		_q_len = strlen(_query) + 1;
	}
	MPI_Barrier(MPI_COMM_WORLD);
	{
		Runtime::EncodeBegin();
	}
	MPI_Bcast(&_q_len, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(_query, _q_len, MPI_CHAR, 0, MPI_COMM_WORLD);

	_q.setQueryStream(std::string(_query));
	Global::Log("after setQueryStream", "");
	_q.buildQueryStringTriples();
	Global::Log("after buildQueryStringTriples", "");
	_q.generateSignature();
	Global::Log("after generateSignature", "");
	_q.Print();
	Global::Log("finish query input");
	{
		Runtime::EncodeEnd();
	}
}
void Global::Shuffle(int _r, int _s, Query& _q){
	Global::Log("In Shuffle", "***");
	{
		Runtime::ShuffleBegin();
	}
	const int varNum = _q.getVertexNum();
	for(int i = 0; i < varNum; i ++)
	{
		int _into_rank = Global::getRankByVarID(i);
		_q.shuffleCandidate(_r, _into_rank, i, _s);
//		if(Global::testShuffle) return;
	}
	{
		Runtime::ShuffleEnd();
	}
	Global::Log("Out Shuffle", "***");
}
void Global::shareVarsDistribution(int _r, int _s, Query & _q){
	Global::Log("In shareVarsDistribution", "***");
	int vars[MAX_QUERY_MAP] = {};
	_q.getVarsInThisNode(vars);
	const int nVar = _q.getVertexNum();

	int gatherVars[MAX_COMPUTE_NODE][MAX_QUERY_MAP] = {};

	MPI_Gather(vars, MAX_QUERY_MAP, MPI_INT, gatherVars, MAX_QUERY_MAP, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Bcast((int*)gatherVars, MAX_COMPUTE_NODE * MAX_QUERY_MAP, MPI_INT, 0, MPI_COMM_WORLD);
	stringstream _gather;

	_gather << "on rank : " << _r << " of size " << _s << "\n";
	for(int i = 0; i < nVar; i ++)
	{
		_gather << i << " lies in = ";
		for(int r = 0; r < _s; r ++)
		{
			if(gatherVars[r][i] == 1)
				_gather << r << " ";
		}
		_gather << "\n";
	}
	Global::Log(_gather.str().c_str(), "");
	memcpy(_q.varsDistribution, gatherVars, sizeof(gatherVars));
	Global::Log("Out shareVarsDistribution", "***");
}
int Global::getRankByVarID(int _varID){
	return (_varID) % MAX_COMPUTE_NODE + 1;
}
int Global::getVarIDByRank(int _rank){
	return (_rank % MAX_QUERY_MAP) - 1;
}
void Global::Retrieve(Query& _q){
	{
		Runtime::RetrieveBegin();
	}
	Global::Stree ->Retrieve(_q);
	{
		Runtime::RetrieveEnd();
	}
}
void Global::loadCandidateEdge(Query& _q){
	{
		Runtime::loadCandidateEdgeBegin();
	}
	_q.loadCandidateEdge();
	{
		Runtime::loadCandidateEdgeEnd();
	}
}



std::string Global::sub2IDTable = "sub2ID";
std::string Global::pre2IDTable = "pre2ID";
std::string Global::obj2IDTable = "obj2ID";
std::string Global::ID2subTable = "ID2sub";
std::string Global::ID2preTable = "ID2pre";
std::string Global::ID2objTable = "ID2obj";
std::string Global::SumEdgeTable = "sumEdge";

std::string Global::streeTable = "sTree";
std::string Global::E2ETable = "E2E";
std::string Global::V2ETable = "V2E";

std::string Global::strCol = "Str:str";
std::string Global::idCol = "ID:id";
std::string Global::ftCol[10] = {
		"level0:ft",
		"level1:ft",
		"level2:ft",
		"level3:ft",
		"level4:ft",
		"level5:ft",
		"level6:ft",
		"level7:ft",
		"level8:ft",
		"level9:ft"
};
std::string Global::sgCol[10] = {
		"level0:sg",
		"level1:sg",
		"level2:sg",
		"level3:sg",
		"level4:sg",
		"level5:sg",
		"level6:sg",
		"level7:sg",
		"level8:sg",
		"level9:sg"
};
std::string Global::szCol[10] = {
		"level0:sz",// in fact there are no level0:sz, just for convenience
		"level1:sz",
		"level2:sz",
		"level3:sz",
		"level4:sz",
		"level5:sz",
		"level6:sz",
		"level7:sz",
		"level8:sz",
		"level9:sz"
};
std::string Global::eSigCol[10] = {
		"level0:eSig",
		"level1:eSig",
		"level2:eSig",
		"level3:eSig",
		"level4:eSig",
		"level5:eSig",
		"level6:eSig",
		"level7:eSig",
		"level8:eSig",
		"level9:eSig"
};
std::string Global::nieCol[10] = {
		"level0:nie",
		"level1:nie",
		"level2:nie",
		"level3:nie",
		"level4:nie",
		"level5:nie",
		"level6:nie",
		"level7:nie",
		"level8:nie",
		"level9:nie"
};
std::string Global::noeCol[10] = {
		"level0:noe",
		"level1:noe",
		"level2:noe",
		"level3:noe",
		"level4:noe",
		"level5:noe",
		"level6:noe",
		"level7:noe",
		"level8:noe",
		"level9:noe"
};
std::string Global::fieCol[10] = {
		"level0:fie",
		"level1:fie",
		"level2:fie",
		"level3:fie",
		"level4:fie",
		"level5:fie",
		"level6:fie",
		"level7:fie",
		"level8:fie",
		"level9:fie"
};
std::string Global::foeCol[10] = {
		"level0:foe",
		"level1:foe",
		"level2:foe",
		"level3:foe",
		"level4:foe",
		"level5:foe",
		"level6:foe",
		"level7:foe",
		"level8:foe",
		"level9:foe"
};
std::string Global::InSizeCol[10] = {
		"level0:insz",
		"level1:insz",
		"level2:insz",
		"level3:insz",
		"level4:insz",
		"level5:insz",
		"level6:insz",
		"level7:insz",
		"level8:insz",
		"level9:insz"
};
std::string Global::OutSizeCol[10] = {
		"level0:outsz",
		"level1:outsz",
		"level2:outsz",
		"level3:outsz",
		"level4:outsz",
		"level5:outsz",
		"level6:outsz",
		"level7:outsz",
		"level8:outsz",
		"level9:outsz"
};
std::string Global::LiteralSizeCol[10] = {
		"level0:lsz",
		"level1:lsz",
		"level2:lsz",
		"level3:lsz",
		"level4:lsz",
		"level5:lsz",
		"level6:lsz",
		"level7:lsz",
		"level8:lsz",
		"level9:lsz"
};
std::string Global::sons[MAX_SON_NUMBER] = {
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
		"11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
		"21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
		"31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
		"41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
		"51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
		"61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
		"71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
		"81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
		"91", "92", "93", "94", "95", "96", "97", "98", "99", "100",
		"101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
		"111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
		"121", "122", "123", "124", "125", "126", "127", "128", "129", "130",
		"131", "132", "133", "134", "135", "136", "137", "138", "139", "140",
		"141", "142", "143", "144", "145", "146", "147", "148", "149", "150",
		"151", "152", "153", "154", "155", "156", "157", "158", "159", "160",
		"161", "162", "163", "164", "165", "166", "167", "168", "169", "170",
		"171", "172", "173", "174", "175", "176", "177", "178", "179", "180",
		"181", "182", "183", "184", "185", "186", "187", "188", "189", "190",
		"191", "192", "193", "194", "195", "196", "197", "198", "199", "200",
		"201", "202", "203", "204", "205", "206", "207", "208", "209", "210",
		"211", "212", "213", "214", "215", "216", "217", "218", "219", "220",
		"221", "222", "223", "224", "225", "226", "227", "228", "229", "230",
		"231", "232", "233", "234", "235", "236", "237", "238", "239", "240",
		"241", "242", "243", "244", "245", "246", "247", "248", "249", "250",
		"251", "252", "253", "254", "255",
};
/*
 *
 */

DatabaseHBase* Global::myDatabase;
sTree* Global::Stree;
FILE * Global::log;
bool Global::testTopLevel;
bool Global::testShuffle;
bool Global::test_a1_loading;
bool Global::test_a2_loading;
bool Global::load_NO_LEAF_edges;
bool Global::test_c1_loading;
bool Global::load_edge;
bool Global::debug_mode;
bool Global::test_sumEdge;
bool Global::runQuery;
bool Global::loadsumEdge;
bool Global::logRetrieve;
bool Global::test_loadTree;
char Global::recvBuffer[50*1000*1000];

int Global::subNumber;
int Global::locationEnable;
const int Global::SUBLEN = 3;
int Global::hashType = 1;
