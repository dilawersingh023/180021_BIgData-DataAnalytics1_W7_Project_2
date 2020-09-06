package weatherDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//Q3. Find out 10-top hottest and coldest day.                                                                                                                           (10 Marks)
//
//Here, it can find that those top 10 days on which maximum and minimum temperature was record in the given sample data set.

public class Q3 {
	//mapper class
	public static class MapForTemp extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

	String rows = value.toString();
	String date = rows.substring(6, 14).replaceAll(" ", "");
	String tempHighValue = rows.substring(38, 45).replaceAll(" ", "");
	String tempLowValue = rows.substring(46, 53).replaceAll(" ", "");
	double highValue = Double.parseDouble(tempHighValue);
	double lowValue = Double.parseDouble(tempLowValue);

	if (lowValue != -9999.0) {
		con.write(new DoubleWritable(lowValue), new Text(date));
	}

	if (highValue != -9999.0) {
		con.write(new DoubleWritable(highValue), new Text(date));
	}

}
}
	
	//reducer class
	public static class ReduceForTemp extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		LinkedHashMap<DoubleWritable, Text> lowValueMap = new LinkedHashMap<DoubleWritable, Text>();
		LinkedHashMap<DoubleWritable, Text> highValueMap = new LinkedHashMap<DoubleWritable, Text>();
		int i = 0;
		
		public void reduce(DoubleWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			double tempValue = key.get();
			String date = values.iterator().next().toString();

			if (i < 10) {
				lowValueMap.put(new DoubleWritable(tempValue), new Text(date));
				++i;
			}
				highValueMap.put(new DoubleWritable(tempValue), new Text(date));
					if (highValueMap.size() > 10) {
					highValueMap.remove(highValueMap.keySet().iterator().next());
				}

			}
		public void cleanup(Context con) throws IOException, InterruptedException {

			con.write(null, new Text("Top 10 Coldest Days:"));
			for (Map.Entry<DoubleWritable, Text> m : lowValueMap.entrySet()) {
				con.write(m.getKey(), m.getValue());
			}
			con.write(null, new Text("Top 10 Hottest Days:"));
			List<DoubleWritable> highKeys = new ArrayList<DoubleWritable>(
					highValueMap.keySet());
			Collections.reverse(highKeys);

			for (int i = 0; i < highKeys.size(); i++) {
				con.write(highKeys.get(i), highValueMap.get(highKeys.get(i)));
			}
		}

	}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q3");
			j.setJarByClass(Q3.class);
			j.setMapperClass(MapForTemp.class);
			j.setReducerClass(ReduceForTemp.class);
			j.setOutputKeyClass(DoubleWritable.class);
			j.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);
				
		}
}
