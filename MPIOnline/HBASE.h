#ifndef __HBASE_H
#define __HBASE_H

#include <string.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include "gen-cpp/Hbase.h"

/**
 * Class to operate Hbase.
 *
 * @author Darran Zhang (codelast.com)
 * @version 11-08-24
 * @declaration These codes are only for non-commercial use, and are distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
 * You must not remove this declaration at any time.
 */

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift;

typedef std::vector<std::string> StrVec;
typedef std::map<std::string,std::string> StrMap;
typedef std::vector<ColumnDescriptor> ColVec;
typedef std::map<std::string,ColumnDescriptor> ColMap;
typedef std::vector<TCell> CellVec;
typedef std::map<std::string,TCell> CellMap;

typedef struct hbaseRet {
  std::string rowValue;
  time_t ts;

  hbaseRet() {
    ts = 0;
  }

} hbaseRet;



class HBASE
{
public:
  HBASE();
  virtual ~HBASE();

private:
	static boost::shared_ptr<TTransport> socket;
	static boost::shared_ptr<TTransport> transport;
	static boost::shared_ptr<TProtocol> protocol;

	static HbaseClient *client;

	static std::string  hbaseServiceHost;
	static int     hbaseServicePort;
	static bool    isConnected;

public:
	// extended
	static 	HbaseClient* & getClient();
	// basic
	static bool  connect();

	static bool  connect(std::string host, int port);

	static bool  disconnect();

	static bool  putRow(const std::string &tableName,
			  const std::string &rowKey,
			  const std::string &column,
			  const std::string &rowValue);

	static bool  getRow(hbaseRet &result,
			  const std::string &tableName,
			  const std::string &rowKey,
			  const std::string &columnName);

	static bool getValue(std::string &_val,
			  const std::string &_table,
			  const std::string &_row,
			  const std::string &_column);

	static bool getRow(std::vector<TRowResult> &_result,
			  const std::string &_table,
			  const std::string &_row);
};

#endif
