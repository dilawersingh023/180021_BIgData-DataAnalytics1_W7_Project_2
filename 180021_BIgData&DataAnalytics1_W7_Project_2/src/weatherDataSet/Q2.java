package weatherDataSet;
import java.io.IOException;

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

//Q2. Find out which day was hot or cold [hot temp>35; cold temp <10].                                                                                 (10 Marks)
//
//Here, it can analyze whether the day was hot or cold based on the given temperature conditions.

public class Q2 {
	//mapper class
	public static class MapForTemp extends Mapper<LongWritable, Text, Text, DoubleWritable> {

public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

	String rows = value.toString();
	String date = rows.substring(6, 14).replaceAll(" ", "");
	String tempHighValue = rows.substring(38, 45).replaceAll(" ", "");
	String tempLowValue = rows.substring(46, 53).replaceAll(" ", "");
	double highestValue = Double.parseDouble(tempHighValue);
	double lowestValue = Double.parseDouble(tempLowValue);

	if (lowestValue < 10.0 && highestValue < 35.0)
		con.write(new Text(date + " Cold Day "), new DoubleWritable(lowestValue));

	else if (highestValue > 35.0)
		con.write(new Text(date + " Hot Day "), new DoubleWritable(highestValue));
  }
}
		//reducer class
	public static class ReduceForTemp extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void setup(Context con) throws IOException, InterruptedException {
			con.write(new Text("Date	Hot/Cold	Max Temp"), null);
		}
		
		public void reduce(Text output, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {

			for (DoubleWritable value : values) {
				double tempValue = value.get();
				if (tempValue != -9999.0)
			con.write(output, new DoubleWritable(tempValue));
	}
  }		
}
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q2");
			j.setJarByClass(Q2.class);
			j.setMapperClass(MapForTemp.class);
			j.setReducerClass(ReduceForTemp.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);
			
		}
}
