package weatherDataSet;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



//Q4. Find out maximum and minimum temperature of each day along with time.                                                                  (10 Marks)
//
//Here, it can analyze and find out maximum and minimum temperature of each day along with time.

public class Q4 {
	//mapper class
	public static class MapForTemp extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			String rows = value.toString();
			String date = rows.substring(6, 14).replaceAll(" ", "");
			String tempValueHigh = rows.substring(38, 45).replaceAll(" ", "");
			String tempValueLow = rows.substring(46, 53).replaceAll(" ", "");
			double highestValue = Double.parseDouble(tempValueHigh);
			double lowestValue = Double.parseDouble(tempValueLow);

			if (highestValue != -9999.0 && lowestValue != -9999.0) {
				con.write(new Text(date), new Text(tempValueHigh + ":" + tempValueLow));
			}
		}
	}
	
		//reducer class
	public static class ReduceForTemp extends Reducer<Text, Text, Text, Text> {

		LinkedHashMap<Text, Text> maps = new LinkedHashMap<Text, Text>();

		public void reduce(Text text, Iterable<Text> values, Context con) throws IOException, InterruptedException {

			for (Text value : values) {
				maps.put(new Text(text), new Text(value));
			}
		}
		public void cleanup(Context con) throws IOException, InterruptedException {
	
			con.write(new Text("Date"), new Text( "Max Temperature	Min Temperature"));

		for (Map.Entry<Text, Text> entry : maps.entrySet()) {
		
			String[] temp = entry.getValue().toString().split(":");

		    con.write(new Text(entry.getKey()), new Text(temp[0] + "	" + temp[1]));
	}
  }
}		
	
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q4");
			j.setJarByClass(Q4.class);
			j.setMapperClass(MapForTemp.class);
			j.setReducerClass(ReduceForTemp.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);	
		}
}
