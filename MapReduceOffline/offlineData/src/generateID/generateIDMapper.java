package generateID;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class generateIDMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	Text mpKey = new Text();
	Text mpVal = new Text();
	public void map(LongWritable key, Text value, Context context) 
	throws IOException, InterruptedException 
	{
		{
//			_ii ++;
//			if(_ii %100 != 0) return;
		}
		String[] _sp = (value.toString()).split("\t");
		mpVal.set(_sp[0]);
		for(int i = 1; i < _sp.length; i++)
		{
			mpKey.set(_sp[i]);
			context.write(mpKey, mpVal);
		}
	}
	private int _ii = 0;
}
