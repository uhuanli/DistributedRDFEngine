package generateID;
import java.io.IOException;
import java.util.HashSet;

import mapreduce.globalConf;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class deleteDupMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	
	Text sub = new Text();
	Text pre = new Text();
	Text obj = new Text();
	Text loc = new Text();
	IntWritable iSub = new IntWritable(1);
	IntWritable iPre = new IntWritable(2);
	IntWritable iObj = new IntWritable(3);
	IntWritable iLoc = new IntWritable(4);
	private globalConf _db = globalConf.Yago;
	public void map(LongWritable key, Text values,
			OutputCollector<Text, IntWritable> output, Reporter reporter) 
	throws IOException 
	{
		if(_db.equals(globalConf.Yago))
		{
			forYago(values, output, reporter);
		}
		else
		if(_db.equals(globalConf.Yago2))
		{
			forYago2(values, output, reporter);
		}
	}
	private void forYago(Text values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{		
		String[] _sp = (values.toString()).split("\t");
		sub.set(_sp[0]);		
		pre.set(_sp[1]);
		obj.set(_sp[2]);
		output.collect(sub, iSub);
		output.collect(pre, iPre);
		output.collect(obj, iObj);
		
	}
	private void forYago2(Text values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
	{		
		String[] _sp = (values.toString()).split("\t");
		sub.set(_sp[0]);		
		pre.set(_sp[1]);
		obj.set(_sp[2]);
		output.collect(sub, iSub);
		output.collect(pre, iPre);
		output.collect(obj, iObj);
		if(_sp[1].equals("hasGeoCoordinates"))
		{
			loc.set(_sp[0]);
			output.collect(loc, iLoc);
		}
	}
}
