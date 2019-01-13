/*
 * Sig.cpp
 *
 *  Created on: 2013-4-3
 *      Author: liyouhuan
 */
#include "Type.h"
#include "Global.h"
Sig::Sig()
{
	id = -1;
	signature.reset();
}
std::string Sig::toString()
{
	stringstream buf;
	buf << id << ":";
	for(int i = 0; i < SIGNATURE_LENGTH; i ++)
	{
		if(signature[i]) buf << " " << i;
	}
	return string(buf.str());
}
void Sig::Print()
{
	Global::Log("In Sig::Print", "");
	Global::Log(this ->toString().c_str());
	Global::Log("Out Sig::Print", "");
}

hEdge::hEdge(){
	this ->eSig.reset();
}
hEdge::hEdge(int _literalID, const bitset<SIGNATURE_LENGTH>& _e_sig){
	this ->linkedNode.id = _literalID;
	this ->eSig = _e_sig;
	this ->linkedNode.signature.reset();
}
hEdge::hEdge(std::string _edge_line){
	stringstream _buf(_edge_line.c_str());
	this ->eSig.reset();
	_buf >> this ->linkedNode.id;
	long long _l[SIG_LONG_NUM] = {};
	//linked signature
	for(int i = 0; i < SIG_LONG_NUM; i ++){
		_buf >> _l[i];
	}
	Global::long_toBitset(this ->linkedNode.signature, _l);
	//edge signature
	for(int i = 0; i < SIG_LONG_NUM; i ++){
		_buf >> _l[i];
	}
	Global::long_toBitset(this ->eSig, _l);
}
// no newline
std::string hEdge::toStringLine(){
	stringstream _buf;
	this ->addStringLine(_buf);
	return _buf.str();
}
void hEdge::addStringLine(stringstream & _buf)const{
	_buf << this ->linkedNode.id << " ";
	long long _l[SIG_LONG_NUM] = {};
	Global::bitset_toLong(this ->linkedNode.signature, _l);
	//linked signature
	for(int i = 0; i < SIG_LONG_NUM; i ++){
		_buf << _l[i] << " ";
	}
	for(int i = 0; i < SIG_LONG_NUM; i ++){
		_l[i] &= 0LL;
	}
	//edge signature
	Global::bitset_toLong(this ->eSig, _l);
	for(int i = 0; i < SIG_LONG_NUM; i ++){
		_buf << _l[i] << " ";
	}
}

bool string_triple::operator <(const string_triple & other) const
{
	if(this->subject < other.subject)
		return true;
	if(this->subject == other.subject && this->predict < other.predict)
		return true;
	if(this->subject == other.subject && this->predict == other.predict &&
		this->object < other.object)
		return true;
	if(this->subject == other.subject && this->predict == other.predict &&
		this->object == other.object && this->location < other.location)
		return true;
	return false;
}

bool string_triple::operator==(const string_triple & other) const
{
	if(this->subject != other.subject)
		return false;
	if(this->predict != other.predict)
		return false;
	if(this->object != other.object)
		return false;
	if(this->location != other.location)
		return false;
	return true;
}
std::string string_triple::toString() const{
	stringstream _buf;
	_buf << "sub:" << subject << "\tpre:" << predict << "\tobj:" << object << "\tloc:" << location;
	return _buf.str();
}
std::string hResult::toString(int varNum)const{
	stringstream _ss;
	_ss << "[ ";
	char _entity[200];
	for(int i = 0; i < varNum; i ++)
	{
		memset(_entity, 0, sizeof(_entity));
		if(result[i] != -3)//-3 means it is an isolated Vertex!
		{
			Global::myDatabase ->getEntityIDToEntity(result[i], _entity);
		}
		else
		{
			strcpy(_entity, "*");
		}
		_ss << _entity << "(" << result[i] << ") , ";

	}
	_ss << "]";
	return _ss.str();
}
void Interval::Log(std::string _log){
	stringstream _tmp_ss;
	_tmp_ss << (_total_consume / 1000000.0) << "s";
	Global::Log(_tmp_ss.str().c_str(), _log.c_str());
}

Interval Runtime::loadTreeTime;
Interval Runtime::loadEdgeTime;

Interval Runtime::loadCandidateEdge;
Interval Runtime::totalRunTime;

Interval Runtime::BFSTime;
Interval Runtime::ShuffleTime;
Interval Runtime::LiteralFilterTime;
Interval Runtime::RetrieveTime;
Interval Runtime::EncodeQueryTime;

void Runtime::loadTreeBegin(){
	loadTreeTime.begin();
}
void Runtime::loadTreeEnd(){
	loadTreeTime.end();
}
void Runtime::loadEdgeBegin(){
	loadEdgeTime.begin();
}
void Runtime::loadEdgeEnd(){
	loadEdgeTime.end();
}

void Runtime::totalRunBegin(){
	totalRunTime.begin();
}
void Runtime::totalRunEnd(){
	totalRunTime.end();
}
void Runtime::loadCandidateEdgeBegin(){
	loadCandidateEdge.begin();
}
void Runtime::loadCandidateEdgeEnd(){
	loadCandidateEdge.end();
}

void Runtime::Initial(){
	loadTreeTime.Initial();
	loadEdgeTime.Initial();

	loadCandidateEdge.Initial();
	totalRunTime.Initial();

	EncodeQueryTime.Initial();
	RetrieveTime.Initial();
	LiteralFilterTime.Initial();
	ShuffleTime.Initial();
	BFSTime.Initial();
}
std::string Runtime::toString(){
	stringstream _buf;
	_buf << "Total run time: " << totalRunTime.getConsumeTime() << "\n";
	_buf << "Encode time consume: " << (EncodeQueryTime.getConsumeTime()) << "\n";
	_buf << "Retrieve time consume: " << (RetrieveTime.getConsumeTime()) << "\n";
	_buf << "LiteralFilter time consume: " << (LiteralFilterTime.getConsumeTime()) << "\n";
	_buf << "Shuffle time consume: " << (ShuffleTime.getConsumeTime()) << "\n";
	_buf << "BFS time consume: " << (BFSTime.getConsumeTime()) << "\n";
	_buf << "loadCandidateEdge: " << (loadCandidateEdge.getConsumeTime()) << "\n";
	_buf << "loadEdge: " << (loadEdgeTime.getConsumeTime()) << "\n";
	_buf << "loadTree: " << (loadTreeTime.getConsumeTime()) << "\n";
	return _buf.str();
}
void Runtime::totalRunCheckPoint(std::string _log){
	double _cur = totalRunTime.curConsume();
	stringstream _tmp_ss;
	_tmp_ss << " consume:" << _cur;
	Global::Log(_tmp_ss.str().c_str(), _log.c_str());
}
void Runtime::TimeLog()
{//time consume
	std::string _time_log = Runtime::toString();
	Global::Log(_time_log.c_str(), "time log:\n");
}
//	static time_t JoinTime;
void Runtime::EncodeBegin(){
	EncodeQueryTime.begin();
}
void Runtime::EncodeEnd(){
	EncodeQueryTime.end();
}
void Runtime::RetrieveBegin(){
	RetrieveTime.begin();
}
void Runtime::RetrieveEnd(){
	RetrieveTime.end();
}
void Runtime::LiteralFilterBegin(){
	LiteralFilterTime.begin();
}
void Runtime::LiteralFilterEnd(){
	LiteralFilterTime.end();
}
void Runtime::ShuffleBegin(){
	ShuffleTime.begin();
}
void Runtime::ShuffleEnd(){
	ShuffleTime.end();
}
void Runtime::BFS_Begin(){
	BFSTime.begin();
}
void Runtime::BFS_End(){
	BFSTime.end();
}


void hNode::setSize(int _sz){
	this ->hdataSize = _sz;
}
int hNode::getLiteralEdgeSize()const{
	return literalEdges.size();
}
int hNode::getInEdgeSize()const{
	return inEdges.size();
}
int hNode::getOutEdgeSize()const{
	return outEdges.size();
}
hNode::hNode(Sig _sig, int _level, int _father){
	level = _level;
	father = _father;
	sig = _sig;
	hdataSize = 0;
	hdata = NULL;
}
hNode::hNode(stringstream& _buf){
	hdataSize = 0;
	this ->resolveNodeFromBuf(_buf);
}
hNode::hNode(std::string _encoded_node){
	hdataSize = 0;
	stringstream _buf(_encoded_node.c_str());
	this ->resolveNodeFromBuf(_buf);
	Global::Log("finish new", "");
}
void hNode::resolveNodeFromBuf(stringstream& _buf){
	long long _l[SIG_LONG_NUM] = {};
	int inSize = 0, outSize = 0, literalSize = 0;
	{//first line
//		Global::Log("first line", "");
		_buf >> this ->level;
		_buf >> this ->father;
		_buf >> this ->sig.id;
		{
			for(int i = 0; i < SIG_LONG_NUM; i ++){
				_buf >> _l[i];
			}
			Global::long_toBitset(this ->sig.signature, _l);
		}
		_buf >> inSize;
		_buf >> outSize;
		_buf >> literalSize;
	}
	//inEdge lines
//	Global::Log("inEdge line", "");
	for(int i = 0; i < inSize; i ++){
		hEdge _h_edge;
		_buf >> _h_edge.linkedNode.id;
		//linkedNode
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.linkedNode.signature, _l);
		//eSig
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.eSig, _l);
		this ->inEdges.push_back(_h_edge);
	}
//	Global::Log("outEdge line", "");
	//outEdge lines
	for(int i = 0; i < outSize; i ++){
		hEdge _h_edge;
		_buf >> _h_edge.linkedNode.id;
		//linkedNode
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.linkedNode.signature, _l);
		//eSig
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.eSig, _l);
		this ->outEdges.push_back(_h_edge);
	}
	//literalEdge lines
	for(int i = 0; i < literalSize; i ++){
		hEdge _h_edge;
		_buf >> _h_edge.linkedNode.id;
		//linkedNode
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.linkedNode.signature, _l);
		//eSig
		for(int i = 0; i < SIG_LONG_NUM; i ++){
			_buf >> _l[i];
		}
		Global::long_toBitset(_h_edge.eSig, _l);
		this ->literalEdges.push_back(_h_edge);
	}
//	Global::Log("finish resolve", "");
}
void hNode::addStringNode(stringstream& _buf)const{
	long long _l[SIG_LONG_NUM] = {};
	Global::bitset_toLong(this ->sig.signature, _l);
	//first line : level father id signature inSize outSize
	_buf << this ->level << " " << this ->father << " " << this ->getID();
	for(int i = 0; i < SIG_LONG_NUM; i ++) _buf << " " << _l[i];
	_buf << " " << this ->inEdges.size() << " "
	     << this ->outEdges.size() << " "
	     << this ->literalEdges.size() << "\n";
	{
		stringstream _ss;
		_ss << this ->getID() << "[InEdge lines(" << this ->inEdges.size() << "),";
		Global::Log_((_ss.str()).c_str(), " ");
	}
	//inEdge lines
	for(std::vector<hEdge>::const_iterator it = this ->inEdges.begin();
			it != this ->inEdges.end(); it ++)
	{
		(*it).addStringLine(_buf);
		_buf << "\n";
	}
	{
		stringstream _ss;
		_ss << "OutEdge lines(" << this ->outEdges.size() << "),";
		Global::Log_((_ss.str()).c_str(), " ");
	}
	//outEdge lines
	for(std::vector<hEdge>::const_iterator it = this ->outEdges.begin();
			it != this ->outEdges.end(); it ++)
	{
		{
			if(Global::debug_mode){
//				if((*it).)
			}
		}
		(*it).addStringLine(_buf);
		_buf << "\n";
	}
	{
		stringstream _ss;
		_ss << "LiteralEdge lines(" << this ->literalEdges.size() << ")]";
		Global::Log(_ss.str().c_str(), "\t");
	}
	//literalEdge lines
	for(std::vector<hEdge>::const_iterator it = this ->literalEdges.begin();
			it != this ->literalEdges.end(); it ++)
	{
		(*it).addStringLine(_buf);
		_buf << "\n";
	}
}
std::string hNode::encode()const{
	stringstream _buf;
	this ->addStringNode(_buf);
	return _buf.str();
}
std::string hNode::toString()const{
	stringstream str_hnode;
	str_hnode << "i: " << this ->getID() << " l: " << this ->getLevel()
			  << " f: " << this ->getFather() << " s: " << this ->getSize() << "=====";
	for(int i = 0; i < this ->getSize(); i ++){
		str_hnode << this ->hdata[i] ->toString() << "\t";
	}
	return str_hnode.str();
}
std::string hNode::toEdgeString()const{
	stringstream _str_edges;
	_str_edges << this ->getID() << "\n";
	_str_edges << "====OUT:" << outEdges.size() << "\t";
	for(int i = 0; i < (int)outEdges.size(); i ++)
	{
		_str_edges << outEdges[i].linkedNode.id << " ";
	}
	_str_edges << "\n";
	{
		if(this ->outEdges.size() == 0)
		{
			stringstream tmp_ss;
			tmp_ss << "outEdges size zero " << this ->getID() << "\n";
			Global::Log(tmp_ss.str().c_str(), "");
		}
	}
//	{
//		long long _l[SIG_LONG_NUM] = {};
//		Global::bitset_toLong(outEdges[outEdges.size() - 1].linkedNode.signature, _l);
//		for(int i = 0; i < SIG_LONG_NUM; i ++)
//			_str_edges << _l[i] << " ";
//		_str_edges << "\n";
//	}
	_str_edges << "====IN:" << inEdges.size() << "\t";
	for(int i = 0; i < (int)inEdges.size(); i ++)
	{
		_str_edges << inEdges[i].linkedNode.id << " ";
	}
	_str_edges << "\n";
	//literalEdge
	_str_edges << "====LITERAL:" << literalEdges.size() << "\t";
	for(int i = 0; i < (int)literalEdges.size(); i ++)
	{
		_str_edges << literalEdges[i].linkedNode.id << " ";
	}
	_str_edges << "\n";
	return _str_edges.str();
}
int hNode::getID()const{
	return this ->sig.id;
}
int hNode::getLevel()const{
	return this ->level;
}
void hNode::loadData(){
	char temp[100];
	sprintf(temp, "%d", this ->getID());
	std::string _id = std::string(temp);
	int _level = level;
	{
		if(_level == 0) return;
	}
	sprintf(temp, "level%d:", _level);
	std::string _levelCol = std::string(temp);
	std::string _val;//sz
	HBASE::getValue(_val, Global::streeTable, _id, Global::szCol[_level]);
	int _sz = atoi(_val.c_str());
	this ->setSize(_sz);
	hdata = new hNodeData*[_sz];
	for(int i = 0; i < _sz; i ++)
	{
		hdata[i] = new hNodeData();
		HBASE::getValue(_val, Global::streeTable, _id, _levelCol + Global::sons[i]);
		Global::getSignature(hdata[i] ->sig, _val);
	}
}
void hNode::addOutEdge(std::string _edge){
	int tmp_i;
	hEdge _h_edge;
	std::string _val;
	int _level = this ->getLevel();
//	sscanf(_edge.c_str(), "%d %d", &tmp_i, &(_sig.id));
	stringstream _ss(_edge.c_str());
	_ss >> tmp_i;//sole difference compared with In one;
	_ss >> _h_edge.linkedNode.id;
	{
		HBASE::getValue(_val, Global::E2ETable, _edge, Global::eSigCol[_level]);
		if(_val == "")
		{
			Global::Log((_edge + ": Bug _val").c_str());
			return;
		}
		Global::getBitset(_h_edge.eSig, _val);
	}
	{
		stringstream _tmp_ss;
		_tmp_ss << _h_edge.linkedNode.id;
		HBASE::getValue(_val, Global::streeTable, _tmp_ss.str(), Global::sgCol[_level]);
		if(_val == "")
		{
			Global::Log((_edge + ": Bug _val2").c_str());
			return;
		}
		Global::getBitset(_h_edge.linkedNode.signature, _val);
	}
	this ->outEdges.push_back(_h_edge);
}
void hNode::addInEdge(std::string _edge){
	hEdge _h_edge;
	std::string _val;
	int _level = this ->getLevel();
//	sscanf(_edge.c_str(), "%d %d", &(_sig.id), &tmp_i);
	stringstream _ss(_edge.c_str());
	_ss >> _h_edge.linkedNode.id;
	{
		HBASE::getValue(_val, Global::E2ETable, _edge, Global::eSigCol[_level]);
		if(_val == "")
		{
			Global::Log((_edge + ": Bug _val").c_str());
			return;
		}
		Global::getBitset(_h_edge.eSig, _val);
	}
	{
		stringstream _tmp_ss;
		_tmp_ss << _h_edge.linkedNode.id;
		HBASE::getValue(_val, Global::streeTable, _tmp_ss.str(), Global::sgCol[_level]);
		Global::getBitset(_h_edge.linkedNode.signature, _val);
	}
	this ->inEdges.push_back(_h_edge);
}
void hNode::loadEdges(){
	if(! Global::load_NO_LEAF_edges)
	{
		if(! this ->isLeafElement()) return;
		if(Global::loadsumEdge)
		{
			this ->loadSumEdges();
		}
		else
		{
			this ->loadInEdges();
			this ->loadOutEdges();
		}

//				if(Global::test_a1_loading)
//				{
//					if(this ->getLevel() == 0)
//					{
//						if(Global::loadsumEdge)
//						{
//							this ->loadSumEdges();
//						}
//						else
//						{
//							this ->loadInEdges();
//							this ->loadOutEdges();
//						}
//					}
//				}
//				else
//				if(Global::load_NO_LEAF_edges)
//				{
//					if(this ->isLeafElement())
//					{
//						if(Global::loadsumEdge)
//						{
//							this ->loadSumEdges();
//						}
//						else
//						{
//							this ->loadInEdges();
//							this ->loadOutEdges();
//						}
//					}
//				}
//				else
//				{
//					if(Global::loadsumEdge)
//					{
//						this ->loadSumEdges();
//					}
//					else
//					{
//						this ->loadInEdges();
//						this ->loadOutEdges();
//					}
//				}
	}
}
void hNode::loadSumEdges(){
	{
		if(Global::test_sumEdge)
		{
			Global::Log("IN loadSumEdges", "***");
		}
		if(inEdges.size() != 0 || outEdges.size() != 0 || literalEdges.size() != 0)
		{
			return;
		}
	}
	std::vector<TRowResult> _trr;
	stringstream _str_id;
	_str_id << this ->getID();
	int _level = this ->getLevel();
	HBASE::getRow(_trr, Global::SumEdgeTable, _str_id.str());
	string _val;
	int _in_sz = 0, _out_sz = 0, _literal_sz = 0;
	{
		//
		_val = _trr[0].columns[Global::InSizeCol[_level]].value;
		if(_val != "")
		{
			stringstream _tmp_ss(_val.c_str());
			_tmp_ss >> _in_sz;
		}
		//
		_val = _trr[0].columns[Global::OutSizeCol[_level]].value;
		if(_val != "")
		{
			stringstream _tmp_ss(_val.c_str());
			_tmp_ss >> _out_sz;
		}
		//
		_val = _trr[0].columns[Global::LiteralSizeCol[_level]].value;
		if(_val != "")
		{
			stringstream _tmp_ss(_val.c_str());
			_tmp_ss >> _literal_sz;
		}
	}
	for(int i = 0; i < _in_sz; i ++)
	{//
		stringstream _tmp_ss;
		_tmp_ss << "level" << _level << ":in" << i;
		std::string _fromid_fromsig_esig = _trr[0].columns[_tmp_ss.str()].value;
		hEdge _h_edge(_fromid_fromsig_esig);
		this ->inEdges.push_back(_h_edge);
	}
	for(int i = 0; i < _out_sz; i ++)
	{//
		stringstream _tmp_ss;
		_tmp_ss << "level" << _level << ":out" << i;
		std::string _toid_tosig_esig = _trr[0].columns[_tmp_ss.str()].value;
		hEdge _h_edge(_toid_tosig_esig);
		this ->outEdges.push_back(_h_edge);
	}
	for(int i = 0; i < _literal_sz; i ++)
	{//
		stringstream _tmp_ss;
		_tmp_ss << "level" << _level << ":l" << i;
		std::string _toid_esig = _trr[0].columns[_tmp_ss.str()].value;
		Sig _sig_temp;//just for get the literalID and eSig more convenient
		Global::getSignature(_sig_temp, _toid_esig);
		hEdge _h_edge(_sig_temp.id, _sig_temp.signature);
		this ->literalEdges.push_back(_h_edge);
	}
	{
		if(Global::test_sumEdge)
		{
			Global::Log("OUT loadSumEdges", "***");
		}
	}
}
void hNode::loadOutEdges(){
	if(! this ->outEdges.empty()) return;
	int _id = this ->getID();
	int _level = this ->getLevel();
	stringstream _str_id;
	_str_id << _id;
	std::string _val;
	std::string _edge;
	{
		HBASE::getValue(_val, Global::V2ETable, _str_id.str(), Global::foeCol[_level]);
		if(_val == "") return;
		_edge = _val;
		this ->addOutEdge(_edge);
	}
	while(true)
	{
		HBASE::getValue(_val, Global::E2ETable, _edge, Global::noeCol[_level]);
		if(_val == "") return;
		_edge = _val;
		this ->addOutEdge(_edge);
	}
}
void hNode::loadInEdges(){
	if(! this ->inEdges.empty()) return;
	int _id = this ->getID();
	int _level = this ->getLevel();
	stringstream _str_id;
	_str_id << _id;
	std::string _val;
	std::string _edge;
	{
		HBASE::getValue(_val, Global::V2ETable, _str_id.str(), Global::fieCol[_level]);
		if(_val == "") return;
		_edge = _val;
		this ->addInEdge(_edge);
	}
	while(true)
	{
		HBASE::getValue(_val, Global::E2ETable, _edge, Global::nieCol[_level]);
		if(_val == "") return;
		_edge = _val;
		this ->addInEdge(_edge);
	}
}
bool hNode::matchInV(const bitset<SIGNATURE_LENGTH> &_inV) const{
	for(std::vector<hEdge>::const_iterator it = this ->inEdges.begin();
				it != this ->inEdges.end(); it ++)
	{
		if((_inV & (*it).linkedNode.signature) == _inV )
			return true;
	}
	return false;
}
bool hNode::matchOutV(const bitset<SIGNATURE_LENGTH> &_outV) const{
	for(std::vector<hEdge>::const_iterator it = this ->outEdges.begin();
				it != this ->outEdges.end(); it ++)
	{
		if((_outV & (*it).linkedNode.signature) == _outV )
			return true;
	}
	return false;
}
//sig of outedge should covers _e_sig
bool hNode::hasOutEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _toID)const{
	for(std::vector<hEdge>::const_iterator it = outEdges.begin();
				it != outEdges.end(); it ++)
	{
		if((*it).linkedNode.id != _toID) continue;
		if( _e_sig != ((*it).eSig & _e_sig) ) continue;
		return true;
	}
	return false;
}
//
bool hNode::hasLiteralEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _literalID)const{
	for(std::vector<hEdge>::const_iterator it = literalEdges.begin();
				it != literalEdges.end(); it ++)
	{
		if((*it).linkedNode.id != _literalID) continue;
		if( _e_sig != ((*it).eSig & _e_sig) ) continue;
		return true;
	}
	return false;
}
//sig of inedge should covers _e_sig
bool hNode::hasInEdge(const bitset<SIGNATURE_LENGTH> & _e_sig, int _fromID)const{
	for(std::vector<hEdge>::const_iterator it = inEdges.begin();
				it != inEdges.end(); it ++)
	{
		if((*it).linkedNode.id != _fromID) continue;
		if( _e_sig != ((*it).eSig & _e_sig) ) continue;
		return true;
	}
	return false;
}
bool hNode::isLeaf()const{
	return level == NodeBottom;
}
bool hNode::isLeafElement()const{
	return level == 0;
}
void hNode::loadRootedTree(){
	stringstream _ss;
	{
		if(Global::test_loadTree){
			Global::Log("loading data...");
		}
	}
	this ->loadData();
	{
		if(Global::test_loadTree){
			Global::Log("finish loading data...");
		}
	}
	for(int i = 0; i < this ->getSize(); i ++){
		if(Global::test_loadTree){
			_ss.str("");
			_ss << hdata[i] ->sig.id;
			Global::Log_("[new son:", _ss.str() + " ");
		}
		hdata[i] ->pSon = new hNode(hdata[i] ->sig, this ->getLevel() - 1, this ->getID());
		if(Global::test_loadTree){
			Global::Log_(" finish newing son]", "\t");
		}
	}
	if(Global::test_loadTree){
		Global::Log("\n");
	}
	if(this ->isLeaf()){
		return;
	}

	//leaf element also get assgined for the new hNode, because it has edges in summary graph!!
	for(int i = 0; i < this ->getSize(); i ++){
		hdata[i] ->pSon ->loadRootedTree();
	}
}

int hNode::getFather()const{
	return father;
}
int hNode::getSize()const{
	return hdataSize;
}
void hNode::Print()const{
	Global::Log(this ->toString().c_str());
}
void hNode::PrintEdges()const{
	Global::Log(this ->toEdgeString().c_str());
}
void hNode::PrintRootedTree(std::string _pre)const{
	Global::Log(this ->toString().c_str(), _pre.c_str());
	if(this ->isLeaf()) return;
	for(int i = 0; i < this ->getSize(); i ++){
		hdata[i] ->pSon ->PrintRootedTree(_pre + "====>");
	}
	return;
}
void hNode::PrintRootedEdges()const{
	this ->PrintEdges();
	if(this ->isLeafElement()) return;
	for(int i = 0; i < this ->getSize(); i ++){
		hdata[i] ->pSon ->PrintRootedEdges();
		{
			break;
		}
	}
}
void hNode::loadRootedEdges(){
	if(Global::test_a1_loading && (this ->getLevel() == 0)){
		if(this ->getID() != 2584268 &&
		   this ->getID() != 3050041 &&
		   this ->getID() != 3428956 &&
		   this ->getID() != 26877)
			return;
	}
	{//loading strategy: all else
		this ->loadEdges();
	}
	if(this ->isLeafElement()) return;

	{
		if(Global::testTopLevel)	return;
		{
			if(Global::test_a1_loading && (this ->getLevel() == 1)){
				if(this ->getID() != 1492802 &&
				   this ->getID() != 2460752 &&
				   this ->getID() != 2955816 &&
				   this ->getID() != 26877)
					return;
			}
		}
	}
	for(int i = 0; i < this ->getSize(); i ++){
		hdata[i] ->pSon ->loadRootedEdges();
	}
}

bool hNode::isCandidate(const bitset<SIGNATURE_LENGTH> &_bs) const{
	bitset<SIGNATURE_LENGTH> _tmp = _bs & this ->sig.signature;
	if(_tmp == _bs)		return true;

	return false;
}
void hNode::getSonCandidates(std::vector<hNode*> & _sc_vec, const bitset<SIGNATURE_LENGTH> &_bs){
	for(int i = 0; i < this ->getSize(); i ++){
		if(hdata[i] ->pSon ->isCandidate(_bs))
		{
			_sc_vec.push_back(hdata[i] ->pSon);
		}
	}
}
std::string hNode::getBitString()const{
	stringstream _buf;
	int ones = 0;
	for(int i = 0; i < SIGNATURE_LENGTH; i ++){
		if(this ->sig.signature[i])
		{
			_buf << i << " ";
			ones ++;
		}
	}
	_buf << "[" << ones << "]";
	return _buf.str();
}

hNodeData::hNodeData(){
	pSon = NULL;
}
hNodeData::~hNodeData(){
	delete pSon;
}
std::string hNodeData::toString()const{
	stringstream _ss;
	_ss << "id:" << sig.id;
	return _ss.str();
}

