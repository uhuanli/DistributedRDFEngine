package generateID;
import java.io.IOException;
import java.util.Iterator;

import mapreduce.oReport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.yaml.snakeyaml.representer.Represent;

import com.sun.org.apache.bcel.internal.generic.NEW;


public class deleteDupReducer extends MapReduceBase 
implements Reducer<Text, IntWritable, Text, IntWritable> 
{

	public Text emVal = new Text();
	
	public void reduce(Text _key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException 
	{
		boolean hasSub = false;
		boolean hasPre = false;
		boolean hasObj = false;
		boolean hasLoc = false;
		int _ival = -1;
		while (values.hasNext()) 
		{
			_ival = ((IntWritable)values.next()).get();
			switch(_ival)
			{
			case 1:
			{
				hasSub = true; break;
			}
			case 2:
			{
				hasPre = true; break;
			}
			case 3:
			{
				hasObj = true; break;
			}
			case 4:
			{
				hasLoc = true; break;
			}
			}
		}
		String _val = null;
		if(hasObj)
		{
			_val = "o";
			{
				if(!hasSub)	reporter.incrCounter(oReport.OBJNumber, 1);
				else		reporter.incrCounter(oReport.ObjINSub, 1);
			}
		}
		if(hasSub)
		{
			_val = "s";  // no +=, for when string is both sub and obj, we think it sub
			reporter.incrCounter(oReport.SubNumber, 1);			
		}
		if(hasPre)
		{
			if(_val != null)
				_val += "\tp";
			else 
				_val = "p";
			reporter.incrCounter(oReport.PRENumber, 1);
		}
		if(hasLoc)
		{
			if(_val != null)
				_val += "\tl";
			else 
				_val = "l";
			reporter.incrCounter(oReport.LOCNumber, 1);
		}
		emVal.set(_val);
		output.collect(_key, emVal);
	}

}
