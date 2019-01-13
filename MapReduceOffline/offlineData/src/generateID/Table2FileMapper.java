package generateID;

import hbase.myHbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Table2FileMapper extends TableMapper<Text, Text>  
{
	private byte[] family = null;
	private byte[] qualifier = null;
	private static long _str2id = 0;
	@Override
	public void setup(Context context)
	{
	}
	@Override
	public void map(ImmutableBytesWritable row, Result value, Context context) 
			throws IOException, InterruptedException 
	{
		String _row = row.toString();
		String _val = new String(value.getValue(Bytes.toBytes("Str"), Bytes.toBytes("str")));
		{
			context.write(new Text(_row), new Text(_val));
		}
	}
}
