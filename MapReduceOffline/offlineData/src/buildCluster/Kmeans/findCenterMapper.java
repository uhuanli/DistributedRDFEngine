package buildCluster.Kmeans;

import java.io.IOException;
import java.util.ArrayList;

import mapreduce.Signature;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class findCenterMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Signature> 
{
	private static ArrayList<mCenter> ctrList = new ArrayList<mCenter>();
	private Text emKey = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Signature> output, Reporter reporter)
	throws IOException 
	{
		/*
		 * sub \t long1 space long2 space ....
		 */
		String _line = values.toString();
		String[] _sp = _line.split("\t");
		Signature _sig = new Signature(_sp[1]);
		mCenter _m = new mCenter(_sp[0], _sig);
		for(mCenter iCenter : ctrList)
		{
			if(_m.dist(iCenter) < offlineDriver.SigCloseDst)
				return;
		}
		{
			reporter.incrCounter(oReport.dupCenter, 1);
		}
		ctrList.add(_m);
		emKey.set(_sp[0]);
		output.collect(emKey, _sig);
	}
	
	public void configure(JobConf _job)
	{
		Configuration _conf = _job;
	}
}
