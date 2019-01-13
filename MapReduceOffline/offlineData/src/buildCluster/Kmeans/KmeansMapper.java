package buildCluster.Kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import mapreduce.Signature;
import mapreduce.oReport;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.eclipse.jdt.internal.core.util.ICacheEnumeration;

public class KmeansMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	private static ArrayList<mCenter> ctrList = new ArrayList<mCenter>();
	private int _level = 0;
	private Text tKey = new Text();
	private Text tVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		{
			_ii ++;
//			if(_ii % 10 != 0) return;
			if(_ii <= 1)
			{
				reporter.incrCounter(oReport.loadCenter, ctrList.size());
				reporter.incrCounter(oReport.loadTimes, 1);
			}
		}
		String _line = values.toString();
		{// no impact for the good designation of construction
//			String[] _sp = _line.split("\t");
//			_line = (new StringBuffer(_sp[0]).append("\t").append(_sp[1])) .toString();
		}
		 mCenter _c = new mCenter(_line);
		 /*
		  * check whether ctrList is empty
		  */
		 int minDist = Signature.getLength() + 1;
		 mCenter _minc = null;
		 for(mCenter iCenter : ctrList)
		 {
			 int _dist = iCenter.dist(_c);
			 if(_dist < minDist)
			 {
				 minDist = _dist;
				 _minc = iCenter;
//				 if(minDist < 5) break;
			 }
			 else {
				if(_dist == 0)
				{
					reporter.incrCounter(oReport.skipZero, 1);
					tKey.set(iCenter.getSub());
					tVal.set(_c.toString());
					output.collect(tKey, tVal);
				}
			}
		 }
		 /*
		  * check whether _minc is null
		  */
		 {
			 reporter.incrCounter(oReport.ClusterDist, minDist);
		 }
		 tKey.set(_minc.getSub());
		 tVal.set(_c.toString());
		 output.collect(tKey, tVal);
	}
	public int _ii = 0;
	/*
	 * 
	 */
	public void configure(JobConf job)
	{
		Configuration _jcf = job;
		_level = (int)_jcf.getLong("level", -1);
		if(!ctrList.isEmpty()) return;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(_jcf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Path _path = null;
		{
			if(job.getLong("iIterator", -1) == 0)
			{
				_path = new Path(offlineDriver.initCenters + "part-00000");
				loadctrList(_path, fs);
			}
			else
			{
				for(int i = 0; i < offlineDriver.centerFileNum[_level]; i ++)
				{
					String _part = "part-000";
					if(i < 10) 
					{
						_part += "0" + i;
					}
					else
					{
						_part += i;
					}
					_path = new Path(offlineDriver.centerPath[_level] + _part);
					loadctrList(_path, fs);
				}
			}
		}
		
	}
	public void loadctrList(Path _path, FileSystem fs)
	{
		BufferedReader icBR = null;
		try {
			icBR = new BufferedReader(new InputStreamReader(fs.open(_path)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] _sp_line;
		String _line;
		try {
			while((_line = icBR.readLine()) != null)
			{
				ctrList.add(new mCenter(_line));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}