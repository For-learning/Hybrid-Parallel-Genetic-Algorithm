package cn.cll.hpga;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.cll.hpgaUtil.City;
import cn.cll.hpgaUtil.Tour;

public class LowerLayerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// �������� [55,66]#[23,45]#[24,45]#[11,78]#[34,25] 0
		// ��ȡ��һ���ļ�������
		String line = value.toString();
		if (line.length() > 6) {
			// �з���һ�е�����Ϊһ����������
			String[] words = StringUtils.split(line, "\t");
			String preWord = words[0];
			Integer adaptation = Integer.parseInt(words[1]);
			// Integer adaptation = 0;
			// Spit citys
			String[] citys = StringUtils.split(preWord, "#");

			// �������е����ݶ�����List������
			List<City> array = new ArrayList<City>();
			for (String city : citys) {
				// ȡ��city������axis��ֵ
				int x = Integer.parseInt(new String(city).substring(1, 3));
				int y = Integer.parseInt(new String(city).substring(4, 6));
				array.add(new City(x, y));
			}

			// ������Ӧ��ֵ
			Tour tour = new Tour(array);
			adaptation = tour.getDistance();

			context.write(new LongWritable(adaptation), new Text(preWord));
		}
	}
}
