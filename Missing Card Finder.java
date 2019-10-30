//Ronny Lim _ CS 644 _ Introduction to Big Data _ Missing Card Finder
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class missingPoker {

	public static class MapMap extends Mapper<LongWritable, Text, Text, IntWritable>{

		Text textKey = new Text();
		Text textValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String alpha = value.toString();
			String[] beta = alpha.split(",");

			textKey.set(beta[0]);
			context.write(textKey, new IntWritable(Integer.parseInt(beta[1])));
		}
	}

	public static class gamma extends Reducer<Text,IntWritable, Text, IntWritable>{

		public void reduce(Text  key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {

			ArrayList<Integer> cList = new ArrayList<Integer>();

			for(IntWritable val : value){
				cList.add(val.get());
			}

			int numberx = 13;

			for(int i = 1; i<=numberx; i++){
				if(!cList.contains(i)){
					context.write(key, new IntWritable(i));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Missing Cards Finder");
		job.setJarByClass(missingPoker.class);
		job.setMapperClass(MapMap.class);
		job.setReducerClass(gamma.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
