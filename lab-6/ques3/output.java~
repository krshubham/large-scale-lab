
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class output{
	public static class Map extends Mapper<Object,Text,LongWritable,Text>{
		//String c = "shubham";
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			context.write(key,value);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		conf.setInt(NLineInputFormat.LINES_PER_MAP, 3);
		Job job = new Job(conf,"output");
		job.setJarByClass(output.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}
}

