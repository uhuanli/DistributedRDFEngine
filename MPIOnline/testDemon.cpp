/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Instructions:
 * 1. Run Thrift to generate the cpp module HBase
 *    thrift --gen cpp ../../../src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
 * 2. Execute {make}.
 * 3. Execute {./DemoClient}.
 */
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <poll.h>
#include<HBASE.h>
#include <iostream>
#include <vector>

#include <boost/lexical_cast.hpp>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "gen-cpp/Hbase.h"
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace apache::hadoop::hbase::thrift;

namespace {

static void
printRow(const std::vector<TRowResult> &rowResult)
{
  for (size_t i = 0; i < rowResult.size(); i++) {
    std::cout << "row: " << rowResult[i].row << ", cols: ";
    for (CellMap::const_iterator it = rowResult[i].columns.begin();
         it != rowResult[i].columns.end(); ++it) {
      std::cout << it->first << " => " << it->second.value << "; ";
    }
    std::cout << std::endl;
  }
}

static void
getRowWC(
HbaseClient _hc,
 std::vector<TRowResult> & _result,
 const std::string& _table,
 const std::string & _row,
 const std::string& _column)
{
//	_hc.getRowWithColumns(_result, _table, _row, _column);
}

static void
printVersions(const std::string &row, const CellVec &versions)
{
  std::cout << "row: " << row << ", values: ";
  for (CellVec::const_iterator it = versions.begin(); it != versions.end(); ++it) {
    std::cout << (*it).value << "; ";
  }
  std::cout << std::endl;
}

}

int
main(int argc, char** argv)
{
  if (argc < 3) {
    std::cerr << "Invalid arguments!\n" << "Usage: DemoClient host port" << std::endl;
     //return -1;
  }

	int _rank, _size;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &_size);

boost::shared_ptr<TTransport> socket(new TSocket("172.31.19.15", boost::lexical_cast<int>("9090")));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  HbaseClient client(protocol);

  try {
    transport->open();
    HBASE::connect();

    std::string t("demo_table");

    //
    // Scan all tables, look for the demo table and delete it.
    //
    std::cout << "scanning tables..." << std::endl;
    StrVec tables;
    client.getTableNames(tables);
    for (StrVec::const_iterator it = tables.begin(); it != tables.end(); ++it) {
      std::cout << "  found: " << *it << std::endl;
      if (t == *it) {
        if (client.isTableEnabled(*it)) {
          std::cout << "    disabling table: " << *it << std::endl;
          client.disableTable(*it);
        }
        std::cout << "    deleting table: " << *it << std::endl;
        client.deleteTable(*it);
      }
    }
    {
    	vector<TRowResult> _r;
//    	getRow(client, _r, string("sTree"), string("1"), string("level0:"))
    	client.get(_r, string("sTree"), string("2684926"), string("level0:"));
    	printRow(_r);
    	string _val;
//    	HBASE::getValue(_val, string("sTree"), string("2684926"), string("level1:sg"));
//	std::cout << "sg: " << _val << std::endl;
//    	HBASE::getValue(_val, string("sTree"), string("2684926"), string("level1:sz"));
//	std::cout << "sz: " << _val << std::endl;
    	//printRow(_r);
    }
	HBASE::disconnect();
    transport->close();
  } catch (const TException &tx) {
    std::cerr << "ERROR: " << tx.what() << std::endl;
  }
	std::cout << "r-" << _rank << "-r" << std::endl;
	MPI_Finalize();
}
