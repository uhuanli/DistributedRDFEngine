package hbase;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;



public class HBASE {
	
	public static Configuration Configure(String ZK)
	{
		Configuration _conf = HBaseConfiguration.create();
		_conf.set("hbase.zookeeper.quorum", ZK);
		return _conf;
	}
	
	/*
	 * 
	 */
	public static boolean existRow(HTable _table, String _row) throws IOException
	{
		Get _get = new Get(Bytes.toBytes(_row));
		Result _r = _table.get(_get);
		return !_r.isEmpty();
	}
	/*
	 * 
	 */
	public static byte[] getValue(HTable _table, String _row, String _family, String _qualifier) throws IOException
	{
		Get _get = new Get(Bytes.toBytes(_row));
		Result _r = _table.get(_get);
		return _r.getValue(Bytes.toBytes(_family), Bytes.toBytes(_qualifier));
	}
	public static Result getRow(HTable _table, String _row) throws IOException
	{
		Get _get = new Get(Bytes.toBytes(_row));
		return _table.get(_get);
	}
    /**
     * 
     * 
     */
    public static void createTable(HBaseAdmin _admin, String _tablename, String[] _cfs) throws IOException {
        if (_admin.tableExists(_tablename)) {
            System.out.println("table: "+_tablename+" already exist");
        }
        else {
            HTableDescriptor tabledesc = new HTableDescriptor(_tablename);
            for (int i = 0; i < _cfs.length; i++) {
                tabledesc.addFamily(new HColumnDescriptor(_cfs[i]));
            }
            _admin.createTable(tabledesc);
            System.out.println("create: "+_tablename+" successfully");
        }        
    }
	/*
	 * 
	 */
    public static void multiPut(HTable _table, ArrayList<Put> _p_list) throws IOException
    {
    	_table.put(_p_list);
    }
    /**
     * 
     */
    public static void addRow(HTable _table, String _row, String[] _fs, String[][] _qs, String[][] _value)
    {
    	{
    		if(_fs.length != _value.length || _fs.length != _qs.length)
    		{
    			System.err.print("value length err\n");
    			System.exit(0);
    		}
    	}
        try {
            Put put = new Put(Bytes.toBytes(_row));
            for (int j = 0; j < _fs.length; j++) 
            {
            	{
            		if(_qs[j].length != _value[j].length)
            		{
            			System.err.print("valueK length err\n");
            			System.exit(0);
            		}
            	}
            	for(int k = 0; k < _qs[j].length; k ++)
                {
                	put.add(Bytes.toBytes(_fs[j]), Bytes.toBytes(_qs[j][k]), Bytes.toBytes(_value[j][k]));
                }
                _table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /*
     * table support multiple puts and also multiOP may improve efficiency
     */
    public static void addRow(HTable _table, String _row, String _family, String _qualifier, String _value) throws IOException
    {
    	Put put = new Put(Bytes.toBytes(_row));
    	put.setWriteToWAL(true);
    	put.add(Bytes.toBytes(_family), Bytes.toBytes(_qualifier), Bytes.toBytes(_value));
    	_table.put(put);
    }
    /*
     * 
     */
    public static void deleteTable(HBaseAdmin _admin, String _table_name) throws IOException
    {
    	if(! _admin.tableExists(_table_name))	return;
    	
    	_admin.disableTable(_table_name);
    	_admin.deleteTable(_table_name);
    }
}
