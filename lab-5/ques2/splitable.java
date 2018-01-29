import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class splitable{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String[] line=value.toString().split(",");
			int i=Integer.parseInt(line[1]);
			context.write(new Text(line[3]), new IntWritable(i));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		conf.set("mapred.max.split.size","10000");
		Job job=new Job(conf,"splitable");
		job.setJarByClass(splitable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);

	}

}

