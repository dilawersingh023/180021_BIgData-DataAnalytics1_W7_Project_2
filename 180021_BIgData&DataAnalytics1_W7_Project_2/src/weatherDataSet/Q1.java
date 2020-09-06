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

//Q1. Find out highest temperature for each year.                                                                                                                     (10 Marks)
//
//Here, it can find out the day for each year when the highest temperature was recorded.

public class Q1 {
	//mapper class
		public static class MapForTemp extends Mapper<LongWritable,Text,Text,DoubleWritable>
		{
			public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
			{
				String rows = value.toString();
				String date = rows.substring(6, 10).replaceAll(" ", "");
				String tempREC = rows.substring(38, 45).replaceAll(" ", "");
				double tempValue = Double.parseDouble(tempREC);

				if (tempValue != -9999.0)
					con.write(new Text(date), new DoubleWritable(tempValue));
			}
		}
		//reducer class
		public static class ReduceForTemp extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
		{
			public void reduce(Text output,Iterable<DoubleWritable> values,Context con)throws IOException, InterruptedException
			{
				double highestValue = Double.NEGATIVE_INFINITY;
				for (DoubleWritable value : values) 
				{
					if (value.get() > highestValue)
						highestValue = value.get();
				}

				con.write(new Text("Highest temperature for year " + output + " = "), new DoubleWritable(highestValue));
			}
			
		}
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration c= new Configuration();
			Job j=Job.getInstance(c,"Q1");
			j.setJarByClass(Q1.class);
			j.setMapperClass(MapForTemp.class);
			j.setReducerClass(ReduceForTemp.class);
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(j,new Path(args[0]));
			FileOutputFormat.setOutputPath(j,new Path(args[1]));
			System.exit(j.waitForCompletion(true)?0:1);
			
		}
}
