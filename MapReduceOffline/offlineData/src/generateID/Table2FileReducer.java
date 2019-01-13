package generateID;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Table2FileReducer extends Reducer<Text, Text, Text, Text>  
{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
	throws IOException, InterruptedException 
	{
		for(Text iT : values)
		{
			context.write(key, iT);
		}
		System.out.print("call\n");
		{
			_ii ++;
			if(_ii <= 1) context.setStatus("haluo\n");
		}
	}
	private int _ii = 0;
}
