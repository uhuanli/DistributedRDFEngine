package buildSignature;
import hbase.myHbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import mapreduce.Signature;
import mapreduce.globalConf;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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

import sun.java2d.pipe.SpanClipRenderer;

import com.sun.org.apache.bcel.internal.generic.ISUB;



public class buildSigMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, IntWritable, Signature> 
{
	public static final int soScale = 1500 * 10000;
	public static HashMap<String, Integer> hmSO2ID = null;
	public static HashMap<String, Integer> hmPre2ID = null;
	public static int subNum = -1;
	private globalConf _db = globalConf.Yago2;
	IntWritable isubID = new IntWritable();
	Signature iSig = null;
	Date _d = new Date();
	public void map(LongWritable key, Text values,
			OutputCollector<IntWritable, Signature> output, Reporter reporter) 
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
	public void forYago(Text values,	OutputCollector<IntWritable, Signature> output, Reporter reporter) throws IOException
	{
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1;
		{
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
			objID = Integer.parseInt(rdfUnit[2]);
		}
		/*
		 * 
		 */
		{
			iSig = new Signature();
			isubID.set(subID);
			{
				
			}
			offlineDriver.buildPreSignature(preID, iSig, 1);
			{
				if(subID == 2584268)
				{
					reporter.setStatus(iSig.bitToString() + "\n");
					System.out.print(iSig.bitToString() + "\n");
					reporter.incrCounter(oReport.testTime, 1);
				}
			}
			offlineDriver.buildEntitySignature(rdfUnit[5], iSig, offlineDriver.hashType, 1);
			output.collect(isubID, iSig);
			iSig = null;
		}
		/*
		 * 
		 */
		if(objID < subNum)
		{
			iSig = new Signature();
			isubID.set(objID);
			offlineDriver.buildPreSignature(preID, iSig, 0);
			offlineDriver.buildEntitySignature(rdfUnit[3], iSig, offlineDriver.hashType, 0);
			output.collect(isubID, iSig);
			iSig = null;
		}
	}
	public void forYago2(Text values,	OutputCollector<IntWritable, Signature> output, Reporter reporter) throws IOException
	{
		String _line = values.toString();
		String [] rdfUnit = _line.split("\t");
		/*
		 * 
		 */		
		int subID = -1, objID = -1, preID = -1;//, locID = -1;
		{
			subID = Integer.parseInt(rdfUnit[0]);
			preID = Integer.parseInt(rdfUnit[1]);
			objID = Integer.parseInt(rdfUnit[2]);
//			locID = Integer.parseInt(rdfUnit[3]);
		}
		/*
		 * 
		 */
		{
			iSig = new Signature();
			isubID.set(subID);
//			offlineDriver.buildLocSignature(rdfUnit[7], locID, iSig, 1);
			offlineDriver.buildPreSignature(preID, iSig, 1);
			offlineDriver.buildEntitySignature(rdfUnit[6], iSig, offlineDriver.hashType, 1);
			output.collect(isubID, iSig);
			iSig = null;
		}
		/*
		 * 
		 */
		if(objID < subNum)
		{
			iSig = new Signature();
			isubID.set(objID);
//			offlineDriver.buildLocSignature(rdfUnit[7], locID, iSig, 0);
			offlineDriver.buildPreSignature(preID, iSig, 0);
			offlineDriver.buildEntitySignature(rdfUnit[4], iSig, offlineDriver.hashType, 0);
			output.collect(isubID, iSig);
			iSig = null;
		}
	}
	public void configure(JobConf _job)
	{
		Configuration _jcf = _job;
		subNum = (int)_jcf.getLong("subNum", -1);
	}
	/*
	 * 
	 */
	public static int test_done = -1;
	int _ii = 0;
}
