package buildSignature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import mapreduce.Signature;
import mapreduce.oReport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.sun.org.apache.bcel.internal.generic.NEW;


public class buildSigReducer extends MapReduceBase 
implements Reducer<IntWritable, Signature, IntWritable, Text> 
{
    /* 
     * 
     */
	public void reduce(IntWritable _key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException 
	{
		{
//			_ii ++;
//			if(_ii % 100000 != 0) return;
		}
		Signature iSignature = new Signature();
		
		boolean test = false;
//		if(_key.get() == 2584268) test = true;
		
		while (values.hasNext()) 
		{
			Signature _tmp_sig = (Signature)(values.next());
			if(test)
			{
				reporter.setStatus(_tmp_sig.bitToString() + "\n");
				System.out.print(_tmp_sig.bitToString() + "\n");
				reporter.incrCounter(oReport.OTHER, 1);
			}
			iSignature.or(_tmp_sig);

			{
				reporter.incrCounter(oReport.SubDup, 1);
			}
		}
		{
			reporter.incrCounter(oReport.SigCount, 1);
			reporter.incrCounter(oReport.SigBit, iSignature.ones());
		}
		output.collect(_key, new Text(iSignature.toString()));
		if(test)
		{
			reporter.setStatus(iSignature.bitToString() + "***\n");
			System.out.print(iSignature.bitToString() + "****\n");
			System.out.print(iSignature.toString() + "****\n");
		}
	}   
    
    public int _ii = 0;

}
