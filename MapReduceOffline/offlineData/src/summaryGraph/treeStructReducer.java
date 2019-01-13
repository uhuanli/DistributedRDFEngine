package summaryGraph;

import hbase.myHbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import mapreduce.offlineDriver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class treeStructReducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, Text>
{
	private int _level = 0;
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		HashSet<Integer> hs = new HashSet<Integer>();
		while (values.hasNext()) 
		{
			hs.add( ((IntWritable)values.next()).get() );
		}
		int [] ID = new int[hs.size()];
		int _i = 0;
		for(Integer I : hs)
		{
			ID[_i] = I.intValue();
			_i ++;
		}
		Arrays.sort(ID);
		{
			String[] _sp = _key.toString().split("_");
			int _id = Integer.parseInt(_sp[1]);
			if(_sp[0].equals("s"))
			{
				int _foi = ID[0];
				myHbase.VFirstOutE(_id, _foi, _level);
				String _cur_edge = offlineDriver.encodeEdge(_id, ID[0]);
				String _next_edge = null;
				for(int i = 1; i < ID.length; i ++)
				{
					_next_edge = offlineDriver.encodeEdge(_id, ID[i]);
					myHbase.ENextOutE(_cur_edge, _next_edge, _level);
					_cur_edge = _next_edge;
				}
			}
			else 
			if(_sp[1].equals("o"))
			{
				int _fii = ID[0];
				myHbase.VFirstInE(_id, _fii, _level);
				String _cur_edge = offlineDriver.encodeEdge(_fii, _id);
				String _next_edge = null;
				for(int i = 1; i < ID.length; i ++)
				{
					_next_edge = offlineDriver.encodeEdge(ID[i], _id);
					myHbase.ENextInE(_cur_edge, _next_edge, _level);
					_cur_edge = _next_edge;
				}
			}
		}
	}
	
	public void configure(JobConf _job)
	{
		_level = 1;
	}

}
