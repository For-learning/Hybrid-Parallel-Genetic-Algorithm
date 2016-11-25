package cn.cll.hpga;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UpperLayerReducer extends Reducer<Text, Text, Text, LongWritable> {

	// key: hello , values : {1,1,1,1,1.....}
	// LongWritable
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			context.write(new Text(value.toString()), new LongWritable(0));
		}
	}
}
