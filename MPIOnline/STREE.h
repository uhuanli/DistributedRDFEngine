/*
 * STREE.h
 *
 *  Created on: 2013-3-30
 *      Author: liyouhuan
 */

#ifndef STREE_H_
#define STREE_H_
#include "HBASE.h"
#include "DEFINE.h"
#include "Type.h"
#include "Query.h"


class sTree{
private:
	int levelNum;
	std::vector<hNode*> rootVec;
	int RootOfThisRank(std::vector<hNode*>& root_vec, int _rank, int _size, int _from_level);

public:
	sTree(int _level_num);
	int getLevelNum();
	int getRootLevel();

	void Retrieve(Query & _q);
	void testEncodehNode();

	void loadStree(int _rank, int _size, int _from_level);
	void loadTreeEdges(int _rank, int _size);
	void Print();
	void PrintTreeEdges();

	void PrintRootSig();
};

#endif /* STREE_H_ */
