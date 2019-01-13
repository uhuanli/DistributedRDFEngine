/*
 * Query.cpp
 *
 *  Created on: 2013-4-7
 *      Author: liyouhuan
 */
#include "Query.h"
#include "Global.h"

void Query::shuffleCandidate(int _cur_rank, int _into_rank, int _varID, int _size){
	Global::Log("INshuffleCandidate", "***");
	{
		if(_cur_rank == 0){
			Global::Log("OUTshuffleCandidate", "0:***");
			return;
		}
	}
	int ShuffleTag = 1023;
	memset(Global::recvBuffer, 0, sizeof(Global::recvBuffer));
	if(Global::debug_mode)
	{
		Global::Log("after def recv_buf");
	}
	//if current node is the one to be sent into, call recv
	if(_cur_rank == _into_rank)
	{
		if(Global::debug_mode)
		{
			stringstream _tmp_ss;
			_tmp_ss << "cur:" << _cur_rank << "; into:" << _into_rank << "; "
				    << "varID:" << _varID << "\n";
			Global::Log(_tmp_ss.str().c_str());
		}
		for(int ir = 1; ir < _size; ir ++)
		{
			if(ir == _cur_rank) continue;
			// if node ir has candidate, then call a recv func to recv from ir
			if(this ->varsDistribution[ir][_varID] == 1)
			{
				MPI_Status _status;
				if(Global::debug_mode)
				{
					stringstream _tmp_ss;
					_tmp_ss << "before recv from r" << ir << "\n";
					Global::Log(_tmp_ss.str().c_str());
				}
				MPI_Recv(Global::recvBuffer, (50*1000*1000-1), MPI_CHAR, ir, ShuffleTag, MPI_COMM_WORLD, &_status);
				stringstream _log_buf;
				_log_buf << "rank " << _cur_rank << " shuffle recv size: ";
				_log_buf << (strlen(Global::recvBuffer)) << " from rank " << ir << "\n";
				Global::Log(_log_buf.str().c_str(), "");
				//
				stringstream recv_buf(Global::recvBuffer);
				this ->resolveShuffleCandidate(recv_buf, _varID);
			}
		}
	}
	else
	{
		if(this ->varsDistribution[_cur_rank][_varID] == 0){
			Global::Log("OUTshuffleCandidate_for_no_cdts", "***");
			return;
		}
		if(Global::debug_mode)
		{
			stringstream _tmp_ss;
			_tmp_ss << "cur:" << _cur_rank << "; into:" << _into_rank << "; "
				    << "varID:" << _varID << "\n";
			Global::Log(_tmp_ss.str().c_str());
		}
		stringstream _buf_candidate;
		this ->getShuffleString(_buf_candidate, _varID);
		const std::string _buf_str = _buf_candidate.str();
		if(Global::debug_mode)
		{
			stringstream _tmp_ss;
			_tmp_ss << "begin send to r" << _into_rank << "\n";
			Global::Log(_tmp_ss.str().c_str());
		}
		MPI_Send(_buf_str.c_str(), _buf_str.length() + 1, MPI_CHAR, _into_rank, ShuffleTag, MPI_COMM_WORLD);
		stringstream _log_buf;
		_log_buf << "rank " << _cur_rank << " shuffle send size: ";
		_log_buf << (_buf_str.length() + 1) << " to rank " << _into_rank << "\n";
		Global::Log(_log_buf.str().c_str(), "");
		this ->candidateVec[_varID].clear();
	}
	Global::Log("OUTshuffleCandidate", "***");
}
void Query::resolveShuffleCandidate(stringstream & _buf, int _varID){
	int _node_num = 0;
	_buf >> _node_num;
	for(int i = 0; i < _node_num; i ++)
	{
		hNode * _pNode = new hNode(_buf);
		this ->candidateVec[_varID].push_back(_pNode);
	}
}
void Query::filteredByLiteralEdge(){
	Global::Log("IN filteredByLiteralEdge", "***");
	{
		Runtime::LiteralFilterBegin();
	}
	for(int ivar = 0; ivar < this ->getVertexNum(); ivar ++)
	{//in fact, only one candidateVec is not empty!
		if(this ->candidateVec[ivar].empty()) continue;

		std::string _predict, _literal;
		for(int iedge = 0; iedge < this ->queryVertexEdgeNum[ivar]; iedge ++)
		{
			string_triple* _triple = &(this ->queryStringTriples[this ->queryVertexEdgeID[ivar][iedge]]);
			_literal = _triple ->object;
			if(_literal[0] == '?')  // is an var not real literal and make sure this is an out edge!
				continue;
			_predict = _triple ->predict;
			int objID = -1, preID = -1;
			Global::myDatabase->getPredictToPreID(_predict.c_str(),preID);
			//entity here only has the ID less than subNum
			Global::myDatabase->getEntityToEntityID(_literal.c_str(),objID);

			if(Global::debug_mode)
			{
				stringstream _tmp_ss;
				_tmp_ss<<"predict: "<<_predict.c_str()<<"\n";
				_tmp_ss<<"literalValue: "<<_literal.c_str()<<"\n";
				_tmp_ss<<"preID: "<<preID<<"\n";
				_tmp_ss<<"objID: "<<objID<<"\n";
				Global::Log(_tmp_ss.str().c_str(), "");
			}
			//is preID = -1 possible? yeah
			bitset<SIGNATURE_LENGTH> _e_sig;
			_e_sig.reset();
			if(preID != -1)
			{
				Global::buildPredictSignature(preID, _e_sig, 1);
			}
			//
			std::vector<hNode*>::iterator it = this ->candidateVec[ivar].begin();
			while(it != this ->candidateVec[ivar].end())
			{
				if(objID < Global::subNumber)
				{
					if((*it) ->hasOutEdge(_e_sig, objID))
					{
						it ++;
						continue;
					}
				}
				else
				{
					if((*it) ->hasLiteralEdge(_e_sig, objID))
					{
						it ++;
						continue;
					}
				}
				it = this ->candidateVec[ivar].erase(it);
			}
		}
	}
	{
		Runtime::LiteralFilterEnd();
	}
	Global::Log("OUT filteredByLiteralEdge", "***");
}
void Query::getShuffleString(stringstream & _buf, int _varID){
	{
		if(Global::debug_mode)
			Global::Log("IN getShuffleString", "***");
	}
	_buf.str("");
	_buf << this ->candidateVec[_varID].size() << " ";
	int _count = 0;
	stringstream _ss;
	for(std::vector<hNode*>::iterator it = this ->candidateVec[_varID].begin();
					it != this ->candidateVec[_varID].end(); it ++)
	{
		if(Global::debug_mode){
//			if()
		}
		(*it) ->addStringNode(_buf);
		if(Global::debug_mode)
		{
			_ss << _count ++ << ":";
			Global::Log_(_ss.str().c_str(), " ");
			_ss.str("");
		}
	}
	{
		if(Global::debug_mode){
			Global::Log("OUT getShuffleString", "***");
		}
	}
}
//check empty: make sure that RS_v1 is not empty, RS_v2 is empty
void Query::addMiddleResultSet(int _v1, int _v2, std::multimap<int, int>& v1_To_v2){
	Global::Log("IN addMiddleResultSet", "***");
	typedef	std::multimap<int, int>::iterator mmiiIT;
	std::vector<hResult> results_to_be_added_back;
	std::vector<hResult>::iterator itRS = this ->resultSet.begin();
	stringstream log_buf;
	while(itRS != this ->resultSet.end()){
		int _cur_v1ID = (*itRS).result[_v1];
		pair< mmiiIT,  mmiiIT> _range_v1_v2 = v1_To_v2.equal_range(_cur_v1ID);
		if(_range_v1_v2.first == _range_v1_v2.second)
		{
			itRS = this ->resultSet.erase(itRS);
			continue;
		}
		mmiiIT _range_left = _range_v1_v2.first;
		mmiiIT _range_right = _range_v1_v2.second;
		//add in the first value of v2
		(*itRS).result[_v2] = _range_left ->second;
//		{
//			log_buf << _range_left ->first << "==" << _range_left ->second << "\t";
//			log_buf << _range_right ->first << "==" << _range_right ->second << "\n[";
//		}
		_range_left ++;
//		{
//			log_buf << _range_left ->first << "==" << _range_left ->second << "]\t[";
//			log_buf << _range_left ->first << "==" << _range_left ->second << "]\n";
//			Global::Log(log_buf.str().c_str());
//			log_buf.str("");
//		}
		//push back the results built by the others value of v2!
//		Global::Log("5");
		for(; _range_left != _range_v1_v2.second; _range_left ++)
		{
//			log_buf << _range_left ->first << "==" << _range_left ->second << "\t";
//			Global::Log(log_buf.str().c_str());
//			log_buf.str("");
//			Global::Log("5-1");
			hResult _hr((*itRS), _v2, _range_left ->second);
//			Global::Log("5-2");
			results_to_be_added_back.push_back(_hr);
//			Global::Log("5-3");
//			Global::Log((*itRS).toString(this ->getVertexNum()).c_str());
		}
//		Global::Log("6");
		itRS ++;
	}
	for(std::vector<hResult>::const_iterator itRS_added_back = results_to_be_added_back.begin();
		itRS_added_back != results_to_be_added_back.end(); itRS_added_back ++)
	{
		this ->resultSet.push_back( (*itRS_added_back) );
	}
	Global::Log("OUT addMiddleResultSet", "***");
}
void Query::master_Add_IsolatedVertex(int _v, const std::string& _isolated_IDs)
{
	stringstream _buf(_isolated_IDs.c_str());
	int _id_size = 0;
	_buf >> _id_size;
	int _tmp_i = 0;
	for(int i = 0; i < _id_size; i ++)
	{
		_buf >> _tmp_i;
		this ->isolatedVertex[_v].push_back(_tmp_i);
	}
//	std::vector<hResult>::iterator itRS = this ->resultSet.begin();
//	while(itRS != this ->resultSet.end())
//	{
//		(*itRS).result[_v] = -3;
//		itRS ++;
//	}
}
void Query::master_Join(int _v1, int _v2, const std::string& _join_result){
	//both resultSet[v1] and resultSet[v2] are empty,
	//if and only if this is the first join_result sent to master, think about it
	bool empty_v1 = this ->isResultSetEmpty[_v1];
	bool empty_v2 = this ->isResultSetEmpty[_v2];
	if(Global::debug_mode)
	{
		stringstream _tmp_ss;
		_tmp_ss << "master JOIN:" << _v1 << "=" << _v2 << " jrs:" << _join_result << "\n";
		Global::Log(_tmp_ss.str().c_str(), "***");
		_tmp_ss.str("");
		_tmp_ss << "empty status : ";
		_tmp_ss << "[" << _v1 << "] is " << empty_v1 << " while: [" << _v2 << "] is " << empty_v2 << "\n";
		Global::Log(_tmp_ss.str().c_str());
	}
	//initial the resultSet directly when first time join
	if(this ->isNeverJoin)
	{
		stringstream log_buf;
		stringstream _tmp_ss(_join_result.c_str());
		int _join_size = 0;
		_tmp_ss >> _join_size;
		if(Global::debug_mode)
		{
			log_buf << "join size:" << _join_size << "\n";
		}
		int tmp_v1 = -1, tmp_v2 = -1;
		for(int i = 0; i < _join_size; i ++)
		{
			_tmp_ss >> tmp_v1;
			_tmp_ss >> tmp_v2;
			if(Global::debug_mode)
			{
				log_buf << "tmpv1=" << tmp_v1 << " " << "tmp_v2=" << tmp_v2 << "\n";
			}
			hResult _hr(_v1, _v2, tmp_v1, tmp_v2);
			if(Global::debug_mode)
			{
				Global::Log(_hr.toString(this ->getVertexNum()).c_str(), "log hr:");
			}
			this ->resultSet.push_back(_hr);
		}
		if(_join_size > 0)//think about no answers
		{
			this ->isResultSetEmpty[_v1] = false;
			this ->isResultSetEmpty[_v2] = false;
			this ->isNeverJoin = false;
		}
	}
	else
	if(empty_v1 && empty_v2)
	{
		stringstream log_buf;
		stringstream _tmp_ss(_join_result.c_str());
		int _join_size = 0;
		_tmp_ss >> _join_size;
		if(Global::debug_mode)
		{
			log_buf << "join size:" << _join_size << "\n";
		}
		int tmp_v1 = -1, tmp_v2 = -1;

		if(_join_size <= 0) return;

		std::vector<hResult> tmpRS;
		tmpRS.assign(this ->resultSet.begin(), this ->resultSet.end());
		this ->resultSet.clear();
		for(int i = 0; i < _join_size; i ++)
		{
			_tmp_ss >> tmp_v1;
			_tmp_ss >> tmp_v2;
			if(Global::debug_mode)
			{
				log_buf << "tmpv1=" << tmp_v1 << " " << "tmp_v2=" << tmp_v2 << "\n";
			}
			for(std::vector<hResult>::iterator it = tmpRS.begin();
				it != tmpRS.end(); it ++)
			{
				hResult _hr((*it));
				_hr.result[_v1] = tmp_v1;
				_hr.result[_v2] = tmp_v2;
				this ->resultSet.push_back(_hr);
			}
		}
		if(_join_size > 0)//think about no answers
		{
			this ->isResultSetEmpty[_v1] = false;
			this ->isResultSetEmpty[_v2] = false;
		}
	}
	else
	if(empty_v1 && !empty_v2)
	{
		stringstream log_buf;
		this ->isResultSetEmpty[_v1] = false;
		stringstream _tmp_ss(_join_result.c_str());
		int _join_size = 0;
		_tmp_ss >> _join_size;
		int tmp_v1 = -1, tmp_v2 = -1;
		multimap<int, int> v2_To_v1;
		for(int i = 0; i < _join_size; i ++)
		{
			_tmp_ss >> tmp_v1;
			_tmp_ss >> tmp_v2;
			if(Global::debug_mode)
			{
				log_buf << "tmpv1=" << tmp_v1 << " " << "tmp_v2=" << tmp_v2 << "\n";
				Global::Log(log_buf.str().c_str());
				log_buf.str("");
			}
			v2_To_v1.insert(pair<int, int>(tmp_v2, tmp_v1));
		}
		//
		this ->addMiddleResultSet(_v2, _v1, v2_To_v1);
//		{
//			stringstream rs_buf;
//			rs_buf << "the ResultSet after join is : ";
//			for(int i = 0; i < (int)resultSet.size(); i ++){
//				rs_buf << resultSet[i].toString(this ->getVertexNum()) << "\n";
//			}
//			rs_buf << "\n";
//			Global::Log(rs_buf.str().c_str(), "");
//		}
	}
	else
	if(!empty_v1 && empty_v2)
	{
		stringstream log_buf;
		this ->isResultSetEmpty[_v2] = false;
		stringstream _tmp_ss(_join_result.c_str());
		int _join_size = 0;
		_tmp_ss >> _join_size;
		int tmp_v1 = -1, tmp_v2 = -1;
		multimap<int, int> v1_To_v2;
		for(int i = 0; i < _join_size; i ++)
		{
			_tmp_ss >> tmp_v1;
			_tmp_ss >> tmp_v2;
			if(Global::debug_mode)
			{
				log_buf << "tmpv1=" << tmp_v1 << " " << "tmp_v2=" << tmp_v2 << "\n";
				Global::Log(log_buf.str().c_str());
				log_buf.str("");
			}
			v1_To_v2.insert(pair<int, int>(tmp_v1, tmp_v2));
		}
		//
		this ->addMiddleResultSet(_v1, _v2, v1_To_v2);
//		{
//			stringstream rs_buf;
//			rs_buf << "the ResultSet after join is : ";
//			for(int i = 0; i < (int)resultSet.size(); i ++){
//				rs_buf << resultSet[i].toString(this ->getVertexNum()) << "\n";
//			}
//			rs_buf << "\n";
//			Global::Log(rs_buf.str().c_str(), "");
//		}
	}
	else
	if(!empty_v1 && !empty_v2)// mapping v1 to v2 is just fine
	{
		set<pair<int, int> > v1_AND_v2;
		{//get the pair set
			stringstream _tmp_ss(_join_result.c_str());
			int _join_size = 0;
			_tmp_ss >> _join_size;
			int tmp_v1 = -1, tmp_v2 = -1;
			for(int i = 0; i < _join_size; i ++)
			{
				_tmp_ss >> tmp_v1;
				_tmp_ss >> tmp_v2;
				v1_AND_v2.insert(pair<int, int>(tmp_v1, tmp_v2));
			}
		}
		std::vector<hResult>::iterator it = this ->resultSet.begin();
		while(it != this ->resultSet.end()){
			int _cur_v1ID = (*it).result[_v1];
			int _cur_v2ID = (*it).result[_v2];
			if(v1_AND_v2.find(pair<int, int>(_cur_v1ID, _cur_v2ID)) == v1_AND_v2.end() )
			{
				it = this ->resultSet.erase(it);
			}
			else
			{
				it ++;
			}
		}//end while
	}
	{
		if(this ->resultSet.empty()){
			Global::Log("resultSet is empty!!!", "!!!");
		}
		else
		if(Global::debug_mode)
		{
			stringstream rs_buf;
			rs_buf << "the ResultSet after join is : ";
			for(int i = 0; i < (int)resultSet.size(); i ++){
				rs_buf << resultSet[i].toString(this ->getVertexNum()) << "\n";
			}
			rs_buf << "\n";
			Global::Log(rs_buf.str().c_str(), "");
		}
	}
	Global::Log("OUT master join", "***");
}
void Query::one_step_Join(int _v1, int _v2,
		const std::string &_IDs_v1_to_v2, std::string& _join_result)
{
	{
		stringstream _tmp_ss;
		_tmp_ss << "one step join:[" << _v1 << "=" << _v2 << "] ids:" << _IDs_v1_to_v2 << "\n";
		Global::Log(_tmp_ss.str().c_str(), "****");
	}
	int _size_IDs = 0;
	stringstream _IDs_buf(_IDs_v1_to_v2.c_str());
	_IDs_buf >> _size_IDs;
	int _cur_v1 = -1;
	std::set<int> v2_candidate_ever_matched;
	v2_candidate_ever_matched.clear();
	stringstream join_pair;
	int join_pair_num = 0;
	for(int i = 0; i < _size_IDs; i ++)
	{
		_IDs_buf >> _cur_v1;
		std::vector<hNode*>::iterator itCV = this ->candidateVec[_v2].begin();
		//visit all candidate of v2 and judge whether some candidate  matches
		//if matches, join them into join_pair
		for(; itCV != this ->candidateVec[_v2].end(); itCV ++)
		{
			const hNode* _pNode = (*itCV);
//			stringstream _buf;
//			_buf << _cur_v1 << " " << _pNode ->getID();
			//if v1 is In neighbor
			if(this ->isEdge[_v1][_v2])
			{
				if(! _pNode ->hasInEdge(this ->edgePredict[_v1][_v2], _cur_v1) )
					continue;
			}
			//if v1 is Out neighbor
			if(this ->isEdge[_v2][_v1])
			{
				if(! _pNode ->hasOutEdge(this ->edgePredict[_v2][_v1], _cur_v1) )
					continue;
			}
			join_pair << " " << _cur_v1 << " " << _pNode ->getID();
			join_pair_num ++;
			v2_candidate_ever_matched.insert(_pNode ->getID());
		}//for itCV
	}//for i

	{
		stringstream _tmp_ss;// num of pair
		_tmp_ss << join_pair_num;
		//get the final one step join result!!!
		_join_result = _tmp_ss.str() + " " + join_pair.str();
	}
	{
		Global::Log(_join_result.c_str(), "oneStepJoin Result:");
	}
	{//filter candidate of v2 that never match v1
		std::vector<hNode*>::iterator itV2 = this ->candidateVec[_v2].begin();
		while(itV2 != this ->candidateVec[_v2].end())
		{
			int _v2_id = (*itV2) ->getID();
			if(v2_candidate_ever_matched.find(_v2_id) == v2_candidate_ever_matched.end())
			{
				itV2 = this ->candidateVec[_v2].erase(itV2);
			}
			else
			{
				itV2 ++;
			}
		}
	}

}
void Query::dealWithIsolatedVertex(int _r, int _v)
{
	int _vRank = Global::getRankByVarID(_v);
	int IsolateTag = 1026;
	if(_r == _vRank)
	{
		std::string _tmp_str = this ->getCandidateIDs(_v);
		MPI_Send(_tmp_str.c_str(), _tmp_str.length() + 1, MPI_CHAR,
				0, IsolateTag, MPI_COMM_WORLD);
		{
			stringstream _tmp_ss;
			_tmp_ss << "send cdts of " << _vRank << " to " << "master" << " \n";
			Global::Log(_tmp_ss.str().c_str());
			Global::Log(_tmp_str.c_str(), "ids is :");
		}
	}
	else
	if(_r == 0)
	{
		char _buffer[1000*1000] = {};
		MPI_Status _status;
		stringstream _tmp_ss;
		{
			_tmp_ss << "recv ids from :" << _vRank << " = ";
			Global::Log(_tmp_ss.str().c_str());
			_tmp_ss.str("");
		}
		MPI_Recv(_buffer, 999999, MPI_CHAR, _vRank, IsolateTag, MPI_COMM_WORLD, &_status);
		{
			_tmp_ss << " " << _buffer << "\n";
			Global::Log(_tmp_ss.str().c_str(), "");
		}
		master_Add_IsolatedVertex(_v, std::string(_buffer));
	}
}
// may be there are (_v1->_v2) and (_v2->_v1) at the meantime!! but just _v1 sent to _v2
void Query::dealWithEdge(int _r, int _v1, int _v2){
	Global::Log("IN dealWithEdge", "***");
	int dealWithEdgeTag = 1024;
	int joinResultTag = 1025;
	int _r1 = Global::getRankByVarID(_v1);
	int _r2 = Global::getRankByVarID(_v2);
	{
		stringstream _tmp_ss;
		_tmp_ss << _r << ": r1=" << _r1 << " r2=" << _r2 << " edge:[" << _v1 << "-" << _v2;
		_tmp_ss << "]\n";
		Global::Log(_tmp_ss.str().c_str());
	}
	char _buffer[20 * 1000 * 1000];
	memset(_buffer, 0, sizeof(_buffer));
	if(_r == _r1)
	{
		std::string _tmp_str = this ->getCandidateIDs(_v1);
		MPI_Send(_tmp_str.c_str(), _tmp_str.length() + 1, MPI_CHAR,
				_r2, dealWithEdgeTag, MPI_COMM_WORLD);
		{
			stringstream _tmp_ss;
			_tmp_ss << "send cdts of " << _v1 << " to " << _v2 << " \n";
			Global::Log(_tmp_ss.str().c_str());
			Global::Log(_tmp_str.c_str(), "ids is :");
		}
	}
	else
	if(_r == _r2)
	{
		MPI_Status _status;
		stringstream _tmp_ss;
		if(Global::debug_mode)
		{
			_tmp_ss << "recv ids from :" << _r1 << " =";
			Global::Log(_tmp_ss.str().c_str());
			_tmp_ss.str("");
		}
		MPI_Recv(_buffer, 999999, MPI_CHAR, _r1, dealWithEdgeTag, MPI_COMM_WORLD, &_status);
		if(Global::debug_mode)
		{
			_tmp_ss << " " << _buffer << "\n";
			Global::Log(_tmp_ss.str().c_str(), "");
		}
		string _join_result;
		this ->one_step_Join(_v1, _v2, std::string(_buffer), _join_result);
		if(Global::debug_mode)
		{
			_tmp_ss.str("");
			_tmp_ss << "get one_step_join result: " << _join_result << "\n";
			Global::Log(_tmp_ss.str().c_str());
		}
		//send the join result to master
		MPI_Send(_join_result.c_str(), _join_result.length() + 1, MPI_CHAR,
				0, joinResultTag, MPI_COMM_WORLD);
		Global::Log("finish send");
	}
	else
	if(_r == 0)
	{
		MPI_Status _status;
		if(Global::debug_mode)
		{
			Global::Log("master recv from: ");
		}
		stringstream _ss;
		if(Global::debug_mode)
		{
			_ss << _r2 << " ";
			Global::Log(_ss.str().c_str());
		}
		MPI_Recv(_buffer, 999999, MPI_CHAR, _r2, joinResultTag, MPI_COMM_WORLD, &_status);
		if(Global::debug_mode)
		{
			_ss.str("");
			_ss << "finish recv from: " << _r2 << " recv buffer: " << _buffer;
			Global::Log(_ss.str().c_str());
			Global::Log("finish recv");
		}
		this ->master_Join(_v1, _v2, std::string(_buffer));
	}
	Global::Log("OUT dealWithEdge", "***");
}
std::string Query::getCandidateIDs(int _i){
	if(this ->candidateVec[_i].empty()){
		Global::Log("bug in getCandidateIDs", "");
		return "0";
	}
	stringstream _IDs_buf;
	_IDs_buf << this ->candidateVec[_i].size() << " ";
	for(std::vector<hNode*>::const_iterator it = this ->candidateVec[_i].begin();
				it != this ->candidateVec[_i].end(); it ++)
	{
		_IDs_buf << (*it) ->getID() << " ";
	}
	return _IDs_buf.str();
}
int Query::get_BFS_start_Root(){
	return 0;//as the root of the QueryGraph
}
bool Query::AllVisited(){
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		if(visitStatus[i] != VISITED) return false;
	}
	return true;
}
bool Query::hasEdge(int _v){
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		if(this ->isEdge[_v][i] || this ->isEdge[i][_v])
			return true;
	}
	return false;
}
int Query::findUnvisited(){
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		if(visitStatus[i] != VISITED) return i;
	}
	return -1;
}
void Query::BFS_VisitQueryGraph(int _r, int _s){
	Global::Log("IN BFS", "***");
	{
		Runtime::BFS_Begin();
	}
	int _root = this ->get_BFS_start_Root();
	std::queue<int> bfsQueue;
	bfsQueue.push(_root);
	while(! bfsQueue.empty())
	{
		int _cur_varID = bfsQueue.front();
		bfsQueue.pop();
		if(! this ->hasEdge(_cur_varID))//isolatedVertex!
		{
			this ->dealWithIsolatedVertex(_r, _cur_varID);
			visitStatus[_cur_varID] = VISITED;
		}
		else
		{
			for(int ivar = 0; ivar < this ->getVertexNum(); ivar ++)
			{
				if(visitStatus[ivar] == VISITED) continue;
				if(this ->isEdge[_cur_varID][ivar] || this ->isEdge[ivar][_cur_varID])
				{
					this ->dealWithEdge(_r, _cur_varID, ivar);
					bfsQueue.push(ivar);
				}
			}
			visitStatus[_cur_varID] = VISITED;
		}

		//other vertex that is not connected with root
		if(bfsQueue.empty() && ! this ->AllVisited())
		{
			int _v_unvisited = this ->findUnvisited();
			bfsQueue.push(_v_unvisited);
			continue;
		}
	}
	if(_r == 0)
	{//clear RS
		std::vector<hResult>::iterator itRS = this ->resultSet.begin();
		while(itRS != this ->resultSet.end()){
			{//tag the isolatedVertex!
				for(int iv = 0; iv < this ->getVertexNum(); iv ++){
					if(! this ->isolatedVertex[iv].empty()){
						(*itRS).result[iv] = -3;//isolatedVertex!
					}
				}
			}
			if((*itRS).invalid(this ->getVertexNum())){
				this ->resultSet.erase(itRS);
			}
			else
			{
				itRS ++;
			}
		}
	}
	{
		Runtime::BFS_End();
	}
	Global::Log("OUT BFS", "***");
}
void Query::PrintResult(int _r){
	Global::Log("IN PRINT RESULT", "***");
	if(_r != 0){
		Global::Log("OUT PRINT RESULT", "***");
		return;
	}
	std::vector<hResult>::iterator it = this ->resultSet.begin();
	stringstream _buf_result;
	_buf_result << "result size: " << this ->resultSet.size() << "\n";
	int _count = 0;
	while(it != this ->resultSet.end()){
		_buf_result << (*it).toString(this ->getVertexNum()) << "\n";
		it ++;
		if(++_count >= 10) break;
	}
	for(int i = 0; i < this ->getVertexNum(); i ++){
		_buf_result << "isolatedSize of " << i << " is "
				    << this ->isolatedVertex[i].size() << "\n";
		for(int iv = 0; iv < (int)this ->isolatedVertex[i].size(); iv ++)
		{
			_buf_result << this ->isolatedVertex[i][iv] << " ";
		}
		_buf_result << "\n\n";
	}
	Global::Log(_buf_result.str().c_str(), "");
	Global::Log("OUT PRINT RESULT", "***");
}
int Query::getVertexNum(){
	return this ->queryVertexNum;
}
bitset<SIGNATURE_LENGTH> Query::getVertexSignature(int _i){
	return this ->queryVertexSignatures[_i];//check _i
}
void Query::addCandidates(int _i, hNode * _pNode){
	this ->candidateVec[_i].push_back(_pNode);
}
void Query::clearCandidate(){
	for(int i = 0; i < this ->getVertexNum(); i ++){
		this ->candidateVec[i].clear();
	}
}
std::vector<hNode*>& Query::getCandidateVec(int _i){
	return this ->candidateVec[_i];
}
void Query::candidateInit(const std::vector<hNode*> & _root_vec){
	Global::Log("In candidate init");
	Global::Stree ->PrintRootSig();
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		for(std::vector<hNode*>::const_iterator it = _root_vec.begin();
						it != _root_vec.end(); it ++)
		{
			if((*it) ->isCandidate(this ->getVertexSignature(i)))
			{
				this ->addCandidates(i, (*it));
			}
			else
			if(Global::testTopLevel && Global::test_a1_loading){
				stringstream _ss;
				_ss << "candidate init " << i << " :";
				_ss << (*it) ->getBitString() << "\n";
			}
		}
	}
	Global::Log("Out candidate init");
}
void Query::getVarsInThisNode(int _vars[]){
	for(int i = 0; i < this ->getVertexNum(); i ++){
		if(this ->candidateVec[i].empty())
			_vars[i] = 0;
		else
			_vars[i] = 1;
	}
}
void Query::neighbourFilter(){
	Global::Log("IN neighbourFilter");
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		if(this ->candidateVec[i].empty())
			continue;
		for(int j = 0; j < this ->getVertexNum(); j ++)
		{
			if(isEdge[i][j])
			{
				const bitset<SIGNATURE_LENGTH>& INsig = this ->queryVertexSignatures[i];
				const bitset<SIGNATURE_LENGTH>& OUTsig = this ->queryVertexSignatures[j];
				this ->outFilter(i, OUTsig);
				this ->inFilter(j, INsig);
			}
		}
	}
	Global::Log("OUT neighbourFilter");
}
void Query::outFilter(int _i, const bitset<SIGNATURE_LENGTH>& _outSig){
	std::vector<hNode*>::iterator it = this ->candidateVec[_i].begin();
	while(it != this ->candidateVec[_i].end())
	{
		if(! (*it) ->matchOutV(_outSig))
		{
			it = this ->candidateVec[_i].erase(it);
		}
		else
		{
			it ++;
		}
	}
}
void Query::inFilter(int _i, const bitset<SIGNATURE_LENGTH>& _inSig){
	std::vector<hNode*>::iterator it = this ->candidateVec[_i].begin();
	while(it != this ->candidateVec[_i].end())
	{
		if(! (*it) ->matchInV(_inSig))
		{
			it = this ->candidateVec[_i].erase(it);
		}
		else
		{
			it ++;
		}
	}
}

void Query::getIntoNextLevel(){
	Global::Log("IN getIntoNextLevel", "***");
	stringstream _tmp_log;
	std::vector<hNode*> preCandidate[MAX_QUERY_MAP];
	{//save preCandidate and clear the CandidateVec array to add in the candidates of the next level;
		for(int i = 0; i < this ->getVertexNum(); i ++)
		{
			preCandidate[i].assign(this ->candidateVec[i].begin(), this ->candidateVec[i].end());
		}
		this ->clearCandidate();
	}

	{//add in the candidates of the next level;
		for(int i = 0; i < this ->getVertexNum(); i ++)
		{
			for(std::vector<hNode*>::const_iterator it = preCandidate[i].begin();
						it != preCandidate[i].end(); it ++)
			{
				(*it) ->getSonCandidates(this ->candidateVec[i], this ->queryVertexSignatures[i]);
			}
		}
	}
	{
		for(int i = 0; i < this ->getVertexNum(); i ++)
		{
			_tmp_log << i << ": [" << preCandidate[i].size() << "=>"
			         << this ->candidateVec[i].size() << "] ";
		}
	}
	Global::Log("OUT getIntoNextLevel", "***");
}

void Query::loadCandidateEdge(){
	for(int i = 0; i < MAX_QUERY_MAP; i ++){
		std::vector<hNode*>::iterator it = this ->candidateVec[i].begin();
		while(it != this ->candidateVec[i].end()){
			(*it) ->loadSumEdges();
			it ++;
		}
	}
}

void Query::getStringTriple(string_triple &_triple, const char* queryLine){
	std::string textline(queryLine);
	textline=textline+"\t";
	std::string::size_type pos=0, prev_pos=0;
	{//subject
		pos=textline.find_first_of('\t',pos);
		_triple.subject = textline.substr(prev_pos,pos-prev_pos);
		prev_pos=++pos;
	}
	{//predict
		pos=textline.find_first_of('\t',pos);
		_triple.predict = textline.substr(prev_pos,pos-prev_pos);
		prev_pos=++pos;
	}
	{//object
		pos=textline.find_first_of('\t',pos);
		_triple.object = textline.substr(prev_pos,pos-prev_pos);
		prev_pos=++pos;
	}
	{//location
		pos=textline.find_first_of('\t',pos);
		if ((int)pos>0){
			_triple.location = textline.substr(prev_pos,pos-prev_pos);
		}
		else{
			_triple.location = "----";
		}
	}
}

std::string Query::getQueryStream(){
	return queryStream;
}
void Query::setQueryStream(std::string _query_stream){
	queryStream = _query_stream;
}
std::string Query::toString(){
	stringstream _buf;
	for(std::map<int, string_triple>::const_iterator it = queryStringTriples.begin();
								it != queryStringTriples.end(); it ++){
		_buf << it ->first << "=>" << ((string_triple)(it ->second)).toString() << "\n";
	}
	return _buf.str();
}
void Query::qLog(){
	Global::Log("IN Q_LOG", "***");
	stringstream _buf;
	for(int i = 0; i < this ->getVertexNum(); i ++)
	{
		int icount = 0;
		_buf << i << ": " << (this ->candidateVec[i].size()) << "\t";
		Global::Log(_buf.str().c_str(), "candidate");
		_buf.str("");
		_buf.clear();
		for(std::vector<hNode*>::const_iterator it = this ->candidateVec[i].begin();
				it != this ->candidateVec[i].end(); it ++)
		{
			_buf << icount ++ << "[" << (*it) ->getID() << "," << (*it) ->getInEdgeSize() << "," << (*it) ->getOutEdgeSize() << "] ";

//			_buf << (*it) ->toEdgeString() << "\n@@@\n";
//			_buf << (*it) ->getBitString() << "\n";
			if(icount % 15 == 0)
			{
				_buf << "\n";
			}
		}
		_buf << "\n";
		Global::Log(_buf.str().c_str(), "");
		_buf.str("");
		_buf.clear();
	}
	Global::Log("OUT Q_LOG", "***");
}
void Query::Print(){
	Global::Log(this ->toString().c_str(), "");
	stringstream _buf;
	_buf<<"queryVertexNum: "<<queryVertexNum<<"\n";
	for (int i=0;i<queryVertexNum;i++){
		_buf<<i<<" "<<queryVertexName[i]<<" "<<queryVertexEdgeNum[i]<<"\n";
		{
			_buf << "signature: ";
			for(int ib = 0; ib < SIGNATURE_LENGTH; ib ++){
				if(queryVertexSignatures[i][ib])
					_buf << ib << " ";
			}
			_buf << "\n";
		}
		for (int j=0;j<queryVertexEdgeNum[i];j++){
			_buf<<queryVertexEdgeType[i][j]<<" "<<queryVertexEdgeID[i][j]<<" "
				<<queryStringTriples[queryVertexEdgeID[i][j]].subject<<" "
				<<queryStringTriples[queryVertexEdgeID[i][j]].predict<<" "
				<<queryStringTriples[queryVertexEdgeID[i][j]].object<<" "
				<<queryStringTriples[queryVertexEdgeID[i][j]].location<<"\n";
		}
	}
	_buf << "\n\n";
	for(int i = 0; i < queryVertexNum; i ++)
	{
		for(int j = 0; j < queryVertexNum; j ++)
		{
			if(isEdge[i][j])
			{
				_buf << "[" << i << "," << j << "] ";
				for(int ibit = 0; ibit < SIGNATURE_LENGTH; ibit ++){
					if(edgePredict[i][j][ibit]) _buf << " " << ibit;
				}
				_buf << "\n";
			}
		}
	}
	Global::Log(_buf.str().c_str(), "");
}
void Query::buildQueryStringTriples(){
	//check queryStream is not ""
	char line[1000];
	queryEdgeNum = 0;
	stringstream _ibuf(this ->getQueryStream().c_str());
	while(_ibuf.getline(line, 999)){
		if(strcmp(line, "end") == 0) break;
		string_triple _s_t;
		this ->getStringTriple(_s_t, line);
		queryStringTriples.insert(std::map<int,string_triple>::value_type(queryEdgeNum,_s_t));
		queryEdgeNum++;
	}
}
void Query::generateSignature(){

	for(int i = 0 ; i < MAX_QUERY_MAP; i ++){
		for(int j = 0;j < MAX_QUERY_MAP; j ++){
			edgePredict[i][j].reset();
			isEdge[i][j] = false;
		}
	}
	for(int i=0;i<queryEdgeNum;i++){
		const string_triple & now_triple = queryStringTriples[i];
		if (now_triple.subject.at(0)=='?'){
			const std::map<std::string,int> ::iterator it = varStr2ID.find(now_triple.subject);
			if(it == varStr2ID.end()){
				varStr2ID[now_triple.subject]= queryVertexNum;
				queryVertexName[queryVertexNum]=now_triple.subject;
				queryVertexNum++;
			}
		}
	}

	for(int i=0;i<queryEdgeNum;i++){
		const string_triple & now_triple = queryStringTriples[i];
		int subID=-1;
		int preID=-1;
		int objID=-1;
		const std::map<std::string,int> ::iterator its = varStr2ID.find(now_triple.subject);
		if(its != varStr2ID.end()){
			subID=its->second;
		}
		const std::map<std::string,int> ::iterator ito = varStr2ID.find(now_triple.object);
		if(ito != varStr2ID.end()){
			objID=ito->second;
		}
		if (subID>=0){
			Global::myDatabase->getPredictToPreID(now_triple.predict.c_str(),preID);
			{
				stringstream _ss;
				_ss << now_triple.predict.c_str() << ":" << preID << "\n";
				Global::Log(_ss.str().c_str(), "");
			}
			if (objID==-1){
				Global::buildEntitySignature(now_triple.object.c_str(),queryVertexSignatures[subID],1);
			}
			Global::buildPredictSignature(preID,queryVertexSignatures[subID],1);
			queryVertexEdgeID[subID][queryVertexEdgeNum[subID]]=i;
			queryVertexEdgeType[subID][queryVertexEdgeNum[subID]]=0;
			queryVertexEdgeNum[subID]++;
		}
		if (objID>=0){
			Global::myDatabase->getPredictToPreID(now_triple.predict.c_str(),preID);
			{
				stringstream _ss;
				_ss << now_triple.predict.c_str() << ":" << preID << "\n";
				Global::Log(_ss.str().c_str(), "");
			}
			if (subID==-1){
				Global::buildEntitySignature(now_triple.object.c_str(),queryVertexSignatures[objID],0);
			}
			Global::buildPredictSignature(preID,queryVertexSignatures[objID],0);
			queryVertexEdgeID[objID][queryVertexEdgeNum[objID]]=i;
			queryVertexEdgeType[objID][queryVertexEdgeNum[objID]]=1;
			queryVertexEdgeNum[objID]++;
		}
		if ((subID>=0)&&(objID>=0)){
			Global::myDatabase->getPredictToPreID(now_triple.predict.c_str(),preID);
			if (preID!=-1){
				Global::buildPredictSignature(preID,edgePredict[subID][objID],1);
				stringstream _ss;
				_ss << now_triple.predict.c_str() << ":" << preID << "\n";
				Global::Log(_ss.str().c_str(), "");
			}
			isEdge[subID][objID] = true;
		}
	}
//	aGraphm.IniGraphm(&aGraphm, edgePredict,isEdge,queryVertexNum);
	cout<<"generateSignature done!"<<endl;
}


Query::Query(){
	queryStringTriples.clear();
	varStr2ID.clear();
	memset(queryVertexSignatures,0,sizeof(queryVertexSignatures));
	for(int i = 0; i < MAX_QUERY_MAP; i ++){
		queryVertexName[i] = "";
		visitStatus[i] = UNVISITED;
		isResultSetEmpty[i] = true;
		isolatedVertex[i].clear();
		candidateVec[i].clear();
	}
	resultSet.clear();
	memset(varsDistribution, 0, sizeof(varsDistribution));
	memset(queryVertexEdgeNum,0,sizeof(queryVertexEdgeNum));
	memset(queryVertexEdgeID,0,sizeof(queryVertexEdgeID));
	memset(queryVertexEdgeType,0,sizeof(queryVertexEdgeType));
	queryVertexNum=0;
	queryEdgeNum = 0;
	queryStream = "";
	isNeverJoin = true;
}
Query::~Query(){
	if(Global::debug_mode)
	{
		Global::Log("~Query");
	}
}
