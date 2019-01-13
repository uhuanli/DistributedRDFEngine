/*
 * STREE.cpp
 *
 *  Created on: 2013-4-2
 *      Author: liyouhuan
 */
#include "Global.h"
#include "STREE.h"


sTree::sTree(int _level_num){
	levelNum = _level_num;
}
//load roots with just Sig
int sTree::RootOfThisRank(std::vector<hNode*>& root_vec, int _rank, int _size, int _from_level){
	{
		stringstream _ss;
		_ss << "In RootOfThisRank: " << _rank << " " << _size << " " << _from_level;
		Global::Log(_ss.str().c_str());
	}
	HbaseClient* client = HBASE::getClient();
	std::vector<std::string> columnNames;
	char temp[100];
	sprintf(temp, "level%d:", _from_level);
	std::string levelCol = std::string(temp);
	columnNames.push_back(levelCol);
	{
		Global::Log((std::string("column push back :") + levelCol).c_str());
	}
	int scanner = client ->scannerOpen(Global::streeTable, "", columnNames);
	{
		Global::Log("open scanner");
	}
	stringstream _buf;
	try
	{
		int _count = 0;
		while (true)
		{
			std::vector<TRowResult> value;
			client ->scannerGet(value, scanner);
//			Global::Log("scannerGet");
			{
				if (value.size() == 0){
					Global::Log("value.size = 0");
					break;
				}
				if(_count % (_size - 1) != (_rank - 1)){//_rank != 0
					_count ++;
					continue;
				}
				_count ++;
			}
			std::string _id = value[value.size() - 1].row;
			if(Global::debug_mode)
			{
				bool _continue = true;
				if((_id == "3385332" ||
				   _id == "3594062" ||
				   _id == "3108851" ||
				   _id == "2932740" )&& Global::test_a1_loading)
				{
					_continue = false;
				}
				if((_id == "170410" ||
				   _id == "2105961" )&&
				   Global::test_c1_loading)
				{
					_continue = false;
				}
				if((_id == "3108851" ||
					_id == "1675455" ||
					_id == "3064605" ||
					_id == "3636507")&& Global::test_a2_loading)
				{
					_continue = false;
				}
				_continue = _continue &&
								(Global::test_a1_loading ||
								 Global::test_c1_loading ||
								 Global::test_a2_loading
								 );
				if(_continue) continue;
				{
					if(!(_id == "3773040" || _id == "2665344"))
						continue;
				}
			}
			std::map<std::string, TCell> s2Cell = value[value.size() - 1].columns;
			std::string _str_sig = s2Cell[Global::sgCol[_from_level]].value;
			Sig _sig;
			Global::getSignature(_sig, _id + "\t" + _str_sig);
			{
//				Global::Log(_id.c_str());
//				_buf << "load:" << _id << " ";
//				for(int i = 0; i < SIGNATURE_LENGTH; i ++)
//				{
//					if(_sig.signature[i]) _buf << i << " ";
//				}
//				_buf << "\n";
			}
			hNode *_p_node = new hNode(_sig, _from_level, -1);//father is -1
			root_vec.push_back(_p_node);
//			break;
		}
		{
			_buf << "load size " << root_vec.size() << " of " << _count;
			Global::Log(_buf.str().c_str());
		}
	}
	catch (const IOError &ioe)
	{
		  std::cout << "FATAL: Scanner raised IOError" << std::endl;
		  Global::Log("FATAL: Scanner raised IOError");
	}
	client ->scannerClose(scanner);
	{
		Global::Log("OUT RootOfThisRank");
	}
	return 0;
}
void sTree::testEncodehNode(){
	if(this ->rootVec.empty())
	{
		Global::Log("rootVec is empty", "");
		return;
	}
	hNode * _pnode = this ->rootVec[0];
	Global::Log("***\nPrint\n***\n", "");
	_pnode ->Print();
	Global::Log("***\nPrintEdges\n***\n", "");
	_pnode ->PrintEdges();
	std::string _node_str = _pnode ->encode();
	Global::Log("***\nPrintEncode\n***\n", "");
	Global::Log(_node_str.c_str(), "");
	Global::Log("***\nNew\n***\n", "");
	hNode * _new_p = new hNode(_node_str);
	Global::Log("***\nPrintNew\n***\n", "");
	_new_p ->Print();
	Global::Log("***\nPrintEdgesNew\n***\n", "");
	_new_p ->PrintEdges();
	Global::Log("***\nfinish testEncodehNode\n***\n", "");

}
void sTree::Retrieve(Query & _q){
	Global::Log("In Retrieve", "***");
	{// initial candidates by roots
		_q.candidateInit(this ->rootVec);

	}
	if(Global::testTopLevel){
		if(Global::logRetrieve){
			_q.qLog();
		}
		Global::Log("Ret Retrieve for test_top_level", "***");
		return;
	}

	int _level = this ->getRootLevel();
	while(_level >= NodeBottom){// #define NodeBottom 1
		{//some filter

		}
		{//get into next level
			_q.getIntoNextLevel();
			if(Global::logRetrieve) _q.qLog();
		}
		{// into next level
			_level --;
		}
	}//end while

	{//for the final result!!

	}
	Global::Log("Out Retrieve", "***");
}

void sTree::loadStree(int _rank, int _size, int _from_level){
	Global::Log("In stree::loadStree", "");
	RootOfThisRank(this ->rootVec, _rank, _size, _from_level);
	{
		if(Global::testTopLevel)	return;
		if(_rank != 0 && Global::test_sumEdge)
		{
			for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++)
			{
				(*it) ->loadSumEdges();
			}
			if(Global::test_sumEdge)
			{
				Global::Log("after test load", "***");
			}
			Global::Log(this ->rootVec[0] ->toString().c_str(), "");
			Global::Log(this ->rootVec[0] ->toEdgeString().c_str(), "");
			Global::Log("Out loadStree", "");
			return;
		}
	}
	int _count = 0;
	for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++){
		printf("loading RootedTree...%d(%d) on rank %d\n", (*it) ->getID(), _count ++, _rank);
		stringstream _tmp_ss;
		_tmp_ss << "loading RootedTree..." << (*it) ->getID();
		Global::Log(_tmp_ss.str().c_str());
		(*it) ->loadRootedTree();
	}
	Global::Log("Out stree::loadStree", "");
}
void sTree::loadTreeEdges(int _rank, int _size){
	Global::Log("IN stree::loadTreeEdges");
	int _count = 0;
	for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++){
		printf("loading TreeEdges...%d(%d) on rank %d\n", (*it) ->getID(), _count ++, _rank);
		stringstream _ss;
		_ss << "loading TreeEdges..." << (*it) ->getID();
		Global::Log(_ss.str().c_str());
		(*it) ->loadRootedEdges();
	}
	Global::Log("OUT stree::loadTreeEdges");
}
void sTree::PrintTreeEdges(){
	for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++){
		(*it) ->PrintRootedEdges();
	}
}
int sTree::getRootLevel(){
	return levelNum - 1;
}
int sTree::getLevelNum(){
	return levelNum;
}
void sTree::Print(){
	for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++){
		(*it) ->PrintRootedTree("");
	}
}
void sTree::PrintRootSig(){
	Global::Log("In print root sig", "***");
	{
		stringstream _tmp_ss;
		_tmp_ss << "rootVec size :" << this ->rootVec.size() << "\n";
		Global::Log(_tmp_ss.str().c_str());
	}
	for(std::vector<hNode*>::const_iterator it = this ->rootVec.begin(); it != this ->rootVec.end(); it ++){
		stringstream _ss;
		_ss << (*it) ->getID() << " ";
		_ss << (*it) ->getBitString() << "\n";
	}
	Global::Log("Out print root sig", "***");
}
