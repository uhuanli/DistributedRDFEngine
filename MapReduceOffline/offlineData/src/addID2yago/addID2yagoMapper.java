package addID2yago;

import hbase.myHbase;
import java.io.IOException;
import java.util.HashMap;
import mapreduce.globalConf;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
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

public class addID2yagoMapper extends MapReduceBase
implements Mapper<LongWritable, Text, IntWritable, Text>
{
	private static HashMap<String, Integer> hmSub2IDCache = new HashMap<String, Integer>(1000*1000);
	private Text tVal = new Text();
	private IntWritable iKey = new IntWritable();
	private int subNumber = -1;
	private globalConf _db = globalConf.Yago;
	public void map(LongWritable key, Text values,
			OutputCollector<IntWritable, Text> output, Reporter reporter)
	throws IOException 
	{
		if(_db.equals(globalConf.Yago2))
		{
			forYago2(values, output, reporter);
		}
		else
		if(_db.equals(globalConf.Yago))
		{
			forYago(values, output, reporter);
		}
	}
	private void forYago(Text values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
	{
		 String[] _sp = values.toString().split("\t");
		 int subID = 0, preID = 0, objID = 0;
		 {
			 Integer _t = hmSub2IDCache.get(_sp[0]);
			 if(_t != null)
			 {
				 subID = _t.intValue();
			 }
			 else
			 {
				 subID = myHbase.getSubIDfromHbase(_sp[0]);
				 hmSub2IDCache.put(_sp[0], subID);
			 }
			 _t = hmSub2IDCache.get(_sp[2]);
			 if(_t != null)
			 {
				 objID = _t.intValue();
			 }
			 else
			 {
				 objID = myHbase.getSubIDfromHbase(_sp[2]);
				 if(objID == -1)	
					 objID = myHbase.getObjIDfromHbase(_sp[2]);
			 }
			 preID = myHbase.getIDByPre(_sp[1]);
			 {
				 if(objID < subNumber)
				 {
					 reporter.incrCounter(oReport.SPSLNumber, 1);
				 }
			 }
		 }
		 StringBuffer _sb = new StringBuffer("");
		 _sb.append(preID).append("\t").append(objID).append("\t")
		 	.append(_sp[0]).append("\t").append(_sp[1]).append("\t")
		 	.append(_sp[2]);
		 iKey.set(subID);
		 tVal.set(_sb.toString());
		 output.collect(iKey, tVal);
	}
	private void forYago2(Text values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
	{
		 String[] _sp = values.toString().split("\t");
		 int subID = 0, preID = 0, objID = 0, locID = 0;
		 {
			 Integer _t = hmSub2IDCache.get(_sp[0]);
			 if(_t != null)
			 {
				 subID = _t.intValue();
			 }
			 else
			 {
				 subID = myHbase.getSubIDfromHbase(_sp[0]);
				 hmSub2IDCache.put(_sp[0], subID);
			 }
			 _t = hmSub2IDCache.get(_sp[2]);
			 if(_t != null)
			 {
				 objID = _t.intValue();
			 }
			 else
			 {
				 objID = myHbase.getSubIDfromHbase(_sp[2]);
				 if(objID == -1)	
					 objID = myHbase.getObjIDfromHbase(_sp[2]);
			 }
			 preID = myHbase.getIDByPre(_sp[1]);
			 locID = myHbase.getIDByLoc(_sp[3]);
			 {
				 //loc may have locID when pre is not hasGeo...
				 if(_sp[1].equals("hasGeoCoordinates"))
				 {
					 myHbase.setLocID2XY(locID, _sp[2]);
					 myHbase.setXY2LocID(_sp[2], locID);
					 reporter.incrCounter(oReport.hasGEO, 1);
				 }
				 if(objID < subNumber)
				 {
					 reporter.incrCounter(oReport.SPSLNumber, 1);
				 }
			 }
		 }
		 StringBuffer _sb = new StringBuffer("");
		 _sb.append(preID).append("\t")
		 	.append(objID).append("\t").append(locID).append("\t")
		 	.append(_sp[0]).append("\t").append(_sp[1]).append("\t")
		 	.append(_sp[2]).append("\t").append(_sp[3]);
		 iKey.set(subID);
		 tVal.set(_sb.toString());
		 output.collect(iKey, tVal);
	}
	public void configure(JobConf _job)
	{
		Configuration _jcf = _job;
		subNumber = (int)_jcf.getLong("subNum", -1);
	}

}
