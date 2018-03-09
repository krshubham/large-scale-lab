import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class uniq{
	public static class Map extends Mapper<LongWritable,Text,Text,LongWritable>{
		private HashSet<Text> names = new HashSet<Text>();
		public void map(Text key, Text value, Context context) throws IOException,InterruptedException{
			String elements = value.toString();
			Text name = new Text(elements);
			names.add(name);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			Iterator<Text> i = names.iterator();
			while (i.hasNext()){
				//System.out.println(i.next());
				context.write(i.next(), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"uniq");
		job.setJarByClass(uniq.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// job.setReducerClass(Reduce.class);
		// job.setNumReducers(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
