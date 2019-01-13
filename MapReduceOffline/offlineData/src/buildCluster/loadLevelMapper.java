package buildCluster;

import hbase.myHbase;

import java.io.IOException;
import java.util.ArrayList;

import mapreduce.oReport;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobconf_005fhistory_jsp;

import buildCluster.Kmeans.mCenter;

public class loadLevelMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	private int level = -1;
	private Text tKey = new Text();
	private Text tVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		ArrayList<mCenter> ctrList = new ArrayList<mCenter>();
		String[] _sp = values.toString().split("\\|");
		mCenter _m = new mCenter(_sp[0]);
		for(int i = 1; i < _sp.length; i ++)
		{
			ctrList.add(new mCenter(_sp[i]));
		}
		myHbase.submitCluster(_m, ctrList, 0, ctrList.size(), level);
		{
			reporter.incrCounter(oReport.CenterNum, 1);
			reporter.incrCounter(oReport.totalSon, ctrList.size());
		}
		tKey.set(_m.getSub());
		tVal.set(_m.getSig().toString());
		output.collect(tKey, tVal);
	}
	public void configure(JobConf _job)
	{
		level = (int) _job.getLong("level", -1);
	}
}