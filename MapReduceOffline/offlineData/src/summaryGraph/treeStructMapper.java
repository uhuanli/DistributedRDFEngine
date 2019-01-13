package summaryGraph;

import java.io.IOException;

import mapreduce.Signature;
import mapreduce.offlineDriver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class treeStructMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable>
{

	private int subNum = -1;
	public void map(LongWritable key, Text values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
	throws IOException 
	{
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1, locID = -1;
		{
			objID = Integer.parseInt(rdfUnit[2]);
			{
				if(objID >= subNum) return;
			}
			subID = Integer.parseInt(rdfUnit[0]);
		}
		/*
		 * 
		 */
		{
			StringBuffer _sb = null;
			_sb = new StringBuffer();
			_sb.append("s_").append(subID);
			output.collect(new Text(_sb.toString()), new IntWritable(objID));
		}
		{
			StringBuffer _sb = null;
			_sb = new StringBuffer();
			_sb.append("o_").append(objID);
			output.collect(new Text(_sb.toString()), new IntWritable(subID));
		}
	}
	
	public void configure(JobConf _job)
	{
		subNum = (int) offlineDriver.subNum;
	}

}