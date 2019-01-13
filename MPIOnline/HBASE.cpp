/*
 * HBASE.cpp
 *
 *  Created on: 2013-3-29
 *      Author: liyouhuan
 */
#include "HBASE.h"
#include "Global.h"
#include<iostream>
/**
 * Class to operate Hbase.
 *
 * @author Darran Zhang (codelast.com)
 * @version 11-08-24
 * @declaration These codes are only for non-commercial use, and are distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
 * You must not remove this declaration at any time.
 */

using namespace std;

boost::shared_ptr<TTransport> HBASE::socket;
boost::shared_ptr<TTransport> HBASE::transport;
boost::shared_ptr<TProtocol> HBASE::protocol;
HbaseClient * HBASE::client;
std::string HBASE::hbaseServiceHost = "172.31.19.15";
int HBASE::hbaseServicePort = 9090;
bool HBASE::isConnected;

HBASE::HBASE()
{

}

HBASE::~HBASE()
{
  if (isConnected) {
    disconnect();
  }
  if (NULL != client) {
    delete client;
    client = NULL;
  }
}

HbaseClient*& HBASE::getClient(){
	if(!isConnected)	connect();

	return client;
}

/**
 * Connect Hbase.
 *
 */
bool HBASE::connect()
{
  if (isConnected) {
    cout << "Already connected, don't need to connect it again" << endl;
    return true;
  }

  try {
    socket.reset(new TSocket(hbaseServiceHost, hbaseServicePort));
    transport.reset(new TBufferedTransport(socket));
    protocol.reset(new TBinaryProtocol(transport));

    client = new HbaseClient(protocol);

    transport->open();
  } catch (const TException &tx) {
    cout << "Connect Hbase error : " << tx.what() << endl;
    return false;
  }

  isConnected = true;
  return isConnected;
}

/**
 * Connect Hbase.
 *
 */
bool HBASE::connect(std::string host, int port)
{
  hbaseServiceHost = host;
  hbaseServicePort = port;

  return connect();
}

/**
 * Disconnect from Hbase.
 *
 */
bool HBASE::disconnect()
{
  if (!isConnected) {
    cout << "Haven't connected to Hbase yet, can't disconnect from it" << endl;
    return false;
  }

  if (NULL != transport) {
    try {
      transport->close();
    } catch (const TException &tx) {
      cout<< tx.what() << endl;
      return false;
    }
  } else {
    return false;
  }

  isConnected = false;
  return true;
}

/**
 * Put a row to Hbase.
 *
 * @param tableName   [IN] The table name.
 * @param rowKey      [IN] The row key.
 * @param column      [IN] The "column family : qualifier".
 * @param rowValue    [IN] The row value.
 * @return True for successfully put the row, false otherwise.
 */
bool HBASE::putRow(const string &tableName, const string &rowKey, const string &column, const string &rowValue)
{
  if (!isConnected) {
    cout << "Haven't connected to Hbase yet, can't put row" << endl;
    return false;
  }

  try {
    std::vector<Mutation> mutations;
    mutations.push_back(Mutation());
    mutations.back().column = column;
    mutations.back().value = rowValue;
    client->mutateRow(tableName, rowKey, mutations);

  } catch (const TException &tx) {
    cout << "Operate Hbase error : " << tx.what() << endl;
    return false;
  }

  return true;
}

/**
 * Get a Hbase row.
 *
 * @param result      [OUT] The object which contains the returned data.
 * @param tableName   [IN] The Hbase table name, e.g. "MyTable".
 * @param rowKey      [IN] The Hbase row key, e.g. "kdr23790".
 * @param columnName  [IN] The "column family : qualifier".
 * @return True for successfully get the row, false otherwise.
 */
bool HBASE::getRow(hbaseRet &result, const std::string &tableName, const std::string &rowKey, const std::string &columnName)
{
  if (!isConnected) {
    cout << "Haven't connected to Hbase yet, can't read data from it" << endl;
    return false;
  }

  std::vector<std::string> columnNames;
  columnNames.push_back(columnName);

  std::vector<TRowResult> rowResult;
  try {
    client->getRowWithColumns(rowResult, tableName, rowKey, columnNames);
  } catch (const TException &tx) {
    cout << "Operate Hbase error : " << tx.what() << endl;
    return false;
  }

  if (0 == rowResult.size()) {
//    cout << "Got no record with the key : [" << rowKey << "]" << endl;
    stringstream _ss;
    _ss << "Got no record with the key : [" << rowKey << "]";
    Global::Log(_ss.str().c_str());
    return false;
  }

  std::map<std::string, TCell>::const_iterator it = rowResult[rowResult.size() -1].columns.begin();
  result.rowValue = it->second.value;
  result.ts = it->second.timestamp;

  return true;
}

bool HBASE::getRow(std::vector<TRowResult> &_result,
			  const std::string &_table,
			  const std::string &_row)
{
	if (!isConnected) {
	//	    cout << "Haven't connected to Hbase yet, can't read data from it" << endl;
	Global::Log("Haven't connected to Hbase yet, can't read data from it");
	return false;
	}
	client ->getRow(_result, _table, _row);
	return true;
}

bool HBASE::getValue(std::string &_val,
		  const std::string &_table,
		  const std::string &_row,
		  const std::string &_column)
{
	if (!isConnected) {
//	cout << "Haven't connected to Hbase yet, can't read data from it" << endl;
	Global::Log("Haven't connected to Hbase yet, can't read data from it");
	return false;
	}
	CellVec _cv;
	client ->get(_cv, _table, _row, _column);
	if(_cv.size() == 0)
	{
//		cout << "get none on row : " << _row << endl;
		stringstream _ss;
		_ss << "get none on row : " << _row << "for col: " << _column;
		Global::Log(_ss.str().c_str());
		_val = "";
		return false;
	}
	_val = _cv[_cv.size() - 1].value;
	return true;
}
