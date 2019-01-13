package generateID;
import java.io.IOException;
import java.util.Iterator;
import mapreduce.offlineDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class generateIDReducer extends TableReducer<Text, Text, ImmutableBytesWritable>
{
	private int subNum = -1;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	// something that need to be done at start of reducer
		Configuration _conf = context.getConfiguration();
		subNum = (int)_conf.getLong("subNum", -1);
	}
	@Override
	public void reduce(Text keyin, Iterable<Text> values, Context context)
	throws IOException 
	{
		String sKey = keyin.toString();
		if(sKey.equals("s"))
		{
			ImmutableBytesWritable putStr2ID = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.sub2ID));
			ImmutableBytesWritable putID2Str = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.ID2sub));
			writeMAP(putStr2ID, putID2Str, values, context, 0);
		}
		else
		if(sKey.equals("p"))
		{
			ImmutableBytesWritable putStr2ID = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.pre2ID));
			ImmutableBytesWritable putID2Str = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.ID2pre));
			writeMAP(putStr2ID, putID2Str, values, context, 0);
		}
		else
		if(sKey.equals("o"))
		{
			ImmutableBytesWritable putStr2ID = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.obj2ID));
			ImmutableBytesWritable putID2Str = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.ID2obj));
			writeMAP(putStr2ID, putID2Str, values, context, subNum);
		}
		else
		if(sKey.equals("l"))
		{
			ImmutableBytesWritable putStr2ID = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.loc2ID));
			ImmutableBytesWritable putID2Str = 
				new ImmutableBytesWritable(Bytes.toBytes(offlineDriver.ID2loc));
			writeMAP(putStr2ID, putID2Str, values, context, 0);
		}
	}
	/*
	 * 
	 */
	public void writeMAP(ImmutableBytesWritable putStr2ID, ImmutableBytesWritable putID2Str,
			Iterable<Text> values, Context context, long baseID) throws IOException
	{
		long _count = baseID;
		for(Text iT : values)
		{
			Put _PutStr2ID = new Put(Bytes.toBytes(iT.toString()));
			_PutStr2ID.add(Bytes.toBytes("ID"), Bytes.toBytes("id"), Bytes.toBytes(_count + ""));
			Put _PutID2Str = new Put(Bytes.toBytes(_count + ""));
			_PutID2Str.add(Bytes.toBytes("Str"), Bytes.toBytes("str"), Bytes.toBytes(iT.toString()));
			try {
				context.write(putStr2ID, _PutStr2ID);
				context.write(putID2Str, _PutID2Str);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			_count ++;
		}
	}
}
