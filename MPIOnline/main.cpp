/*
 * main.cpp
 *
 *  Created on: 2013-3-28
 *      Author: liyouhuan
 */
#include<iostream>
#include<HBASE.h>
#include<mpi.h>
#include"Global.h"
using namespace std;
const int yago = 1;
const int yago2 = 2;
bool testRetrieve = false;
void configure(int _test_mode){

	if(_test_mode == yago)
	{//test parameter
		Global::subNumber = 4339591;
		Global::testTopLevel = 					false;
		Global::testShuffle = 					false;
		Global::test_a1_loading = 				false;
		Global::test_a2_loading = 				false;
		Global::test_c1_loading = 				false;
		Global::load_NO_LEAF_edges = 			false;
		Global::load_edge = 				true;
		Global::debug_mode = 					false;
		Global::test_sumEdge = 					false;
		Global::runQuery = 					true;
		Global::loadsumEdge = 				true;
		Global::logRetrieve = 					false;
		testRetrieve = 							false;
	}
	else
	if(_test_mode == yago2){
		Global::subNumber = 10557352;
		Global::testTopLevel = 					false;
		Global::testShuffle = 					false;
		Global::test_a1_loading = 				false;
		Global::test_a2_loading = 				false;
		Global::test_c1_loading = 				false;
		Global::load_NO_LEAF_edges = 			false;
		Global::load_edge = 					false;
		Global::debug_mode = 				true;
		Global::test_sumEdge = 					false;
		Global::runQuery = 					true;
		Global::loadsumEdge = 					false;
		Global::logRetrieve = 					false;
		testRetrieve = 							false;
		Global::test_loadTree =             true;
	}
}
int main(int argc, char ** argv)
{
	int _rank, _size;
	char _processor[MPI_MAX_PROCESSOR_NAME];
	char _node[1000];
	int _pro_name_len = -1;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &_size);
	MPI_Get_processor_name(_node, &_pro_name_len);
	{
		stringstream _buf;
		_buf << "r" << _rank << "_" << _node;
		strcpy(_processor, _buf.str().c_str());
	}
	Global::Initial(_processor);
	{
		configure(yago2);
	}
	{//load stree: qsort
		Global::loadStree(_rank, _size);
		{

			if(Global::test_sumEdge){
				Global::Log("IN test_sumEdge");
				Global::Finalize(_processor);
				MPI_Finalize();
				return 0;
			}
		}
		if(Global::load_edge)	Global::loadEdges(_rank, _size);
	}

	while(Global::runQuery)
	{
		Global::Log("new while", "***");
		Query myQuery;
		{
			MPI_Barrier(MPI_COMM_WORLD);
			Global::QueryInput(_rank, _size, myQuery);
			Runtime::Initial();
			Runtime::totalRunBegin();
		}
		if(Global::runQuery)
		{//query
			//retrieve
			Runtime::totalRunCheckPoint("beforeRetrieve");
			Global::Retrieve(myQuery);
			//*filter by one step neighbor
			Global::loadCandidateEdge(myQuery);
			//*filter by literal edge
			myQuery.qLog();
			Runtime::totalRunCheckPoint("beforeFileteredByLiteralEdge");
			{
				if(Global::testTopLevel) break;
				if(testRetrieve) break;
			}
			myQuery.filteredByLiteralEdge();
			myQuery.qLog();
			//*filter by one step neighbor var
			Runtime::totalRunCheckPoint("beforeNeighbourFilter");
			myQuery.neighbourFilter();
			myQuery.qLog();
			Runtime::totalRunCheckPoint("beforeShareVarsDistribution");
			Global::shareVarsDistribution(_rank, _size, myQuery);
			//*shuffle
			Runtime::totalRunCheckPoint("beforeShuffle");
			Global::Shuffle(_rank, _size, myQuery);
			//*BFS
			Runtime::totalRunCheckPoint("beforeBFS");
			myQuery.BFS_VisitQueryGraph(_rank, _size);
			//*get result
			Runtime::totalRunCheckPoint("beforeCheckPoint");
			myQuery.PrintResult(_rank);
		}
		{
			Runtime::totalRunEnd();
			Runtime::TimeLog();
		}
		{
			Global::Log("next while");
			MPI_Barrier(MPI_COMM_WORLD);
		}
	}
	Global::Finalize(_processor);
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	return 0;
}
