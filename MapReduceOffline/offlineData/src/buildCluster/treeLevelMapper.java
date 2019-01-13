package buildCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import mapreduce.Signature;
import mapreduce.offlineDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import buildCluster.Kmeans.mCenter;

public class treeLevelMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text>
{
	private static ArrayList<mCenter> ctrList = new ArrayList<mCenter>();
	private int level = -1;
	private Text tKey = new Text();
	private Text tVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException 
	{
		{
//			_ii ++;
//			if(_ii % 10 != 0) return;
		}
		 mCenter _c = new mCenter(values.toString());
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
			 }
		 }
		 /*
		  * check whether _minc is null
		  */
		 tKey.set(_minc.getSub());
		 tVal.set(_c.toString());
		 output.collect(tKey, tVal);
	}
	private int _ii = 0;
	/*
	 * 
	 */
	public void configure(JobConf job)
	{
		Configuration _jcf = job;
		level = (int) _jcf.getLong("level", -1);
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
			for(int i = 0; i < offlineDriver.centerFileNum[level]; i ++)
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
				try {
					if(fs.exists(new Path(offlineDriver.centerPath[level])))
					{
						_path = new Path(offlineDriver.centerPath[level] + _part);
					}
					else
					{
						_path = new Path(offlineDriver.ctrOutput + _part);
					}
					loadctrList(_path, fs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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