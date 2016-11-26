package cn.cll.hpga;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LowerLayerReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// 数据样本 623 [55,66]#[23,45]#[24,45]#[11,78]#[34,25]

		// 直接写出
		for (Text value : values) {
			context.write(value, key);
		}

	}

}
