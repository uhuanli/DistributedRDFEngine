package generateID;

import hbase.myHbase;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.offlineDriver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class f_generateIDReducer extends MapReduceBase
implements Reducer<Text, Text, Text, Text>
{
	private int subNum = -1;
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		int _count = 0;
		boolean isSub = false;
		boolean isObj = false;
		boolean isPre = false;
		boolean isLoc = false;
		{
			if(_key.toString().equals("s")){
				isSub = true;
			}
			if(_key.toString().equals("p")){
				isPre = true;
			}
			if(_key.toString().equals("o")){
				_count = subNum;
				isObj = true;
			}
			if(_key.toString().equals("l")){
				isLoc = true;
			}
		}
		while (values.hasNext()) 
		{
			String _str = ((Text)(values.next())).toString();
			{
				output.collect(_key, _str + "\t" + _count);
			}
			_count ++;
		}
	}
	public void configure(JobConf _job)
	{
		subNum = (int)_job.getLong("subNum", -1);
	}
}
