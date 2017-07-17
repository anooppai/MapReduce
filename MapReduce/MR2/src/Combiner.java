/*import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Combiner {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, CompositeWritable>{


		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			Text station = new Text();
			// private Text type = new Text();
			String observationType;

			int count_min = 0;
			int count_max = 0;
			String type = null;
			String stationID;
			int temperature= 9999;
			String line = value.toString();
			String[] data = line.split(",");
			stationID = data[0];
			station.set(stationID);
			observationType = data[2];
			if(observationType.equals("TMAX")) {
				temperature = Integer.parseInt(data[3]);
				
				type = "TMAX";

			}

			if(observationType.equals("TMIN")){
				temperature = Integer.parseInt(data[3]);
				type = "TMIN";
			}
			if(temperature != 9999)
				context.write(station, new CompositeWritable(temperature, type, 1));

			// StringTokenizer itr = new StringTokenizer(value.toString());
			//while (itr.hasMoreTokens()) {
			//word.set(itr.nextToken());
			//context.write(word, one);
			//}
		}
	}

	public static class IntSumReducer extends Reducer<Text,CompositeWritable,Text,CompositeWritable> 
	{ 
		private IntWritable result = new IntWritable(); 
		public void reduce(Text key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException 
		{ 
			int count_min =0, count_max =0;
			int avg_min =0, avg_max=0;
			int temp_max =0, temp_min=0;
			CompositeWritable out_max= new CompositeWritable();
			CompositeWritable out_min= new CompositeWritable();
			for (CompositeWritable val : values) {
				//max_temp += val.get();
				if ((val.getType()).equals("TMAX"))
				{
					out_max.merge(val);
					out_max.setCount(val);
					out_max.setType("TMAX");
					//count_max++;

				}
				if ((val.getType()).equals("TMIN"))
				{
					out_min.merge(val);
					out_min.setCount(val);
					out_min.setType("TMIN");
					//count_min++;
				}

			}
			context.write(key, out_max);
			context.write(key, out_min);
		}
	}



	public static class Reduce
	extends Reducer<Text,CompositeWritable,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<CompositeWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			//int max_temp = 0;
			int count_min =0, count_max =0;
			int avg_min =0, avg_max=0;
			int temp_max =0, temp_min=0;
			CompositeWritable out_max= new CompositeWritable();
			CompositeWritable out_min= new CompositeWritable();
			for (CompositeWritable val : values) {
				//max_temp += val.get();
				if ((val.getType()).equals("TMAX"))
				{
					out_max.merge(val);
					count_max = val.getCount();
					out_max.setCount(val);
				}
				if ((val.getType()).equals("TMIN"))
				{
					out_min.merge(val);
					count_min = val.getCount();
					out_min.setCount(val);
				}

			}
			temp_max = out_max.getValue();
			temp_min = out_min.getValue();
			count_min = out_min.getCount();
			count_max = out_max.getCount();

			if(count_min != 0)
				avg_min = temp_min / count_min;

			if(count_max != 0)
				avg_max = temp_max / count_max; 

			String output = " "+avg_min+" "+avg_max;
			//Text output = new Text();
			result.set(output);
			//result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "no combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompositeWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
*/


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Combiner {

	//A Mapper class
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, CompositeWritable>{

		//A Map function
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			Text station = new Text();
			String observationType;
			String type = null;
			String stationID;
			int temperature= 9999;
			String line = value.toString();
			String[] data = line.split(",");
			stationID = data[0];
			station.set(stationID);
			observationType = data[2];
			if(observationType.equals("TMAX")) { 					//Checks if the type of record is TMAX
				temperature = Integer.parseInt(data[3]);			//Parses temperature value from the input record
				type = "TMAX";

			}

			if(observationType.equals("TMIN")){						//Checks if the type of record is TMIN
				temperature = Integer.parseInt(data[3]);			//Parses temperature value from the input record
				type = "TMIN";
			}
			if(temperature != 9999)
				// Emits (stationID, temperature, type and count values) only if a TMIN or TMAX record was found
				context.write(station, new CompositeWritable(temperature, type, 1));
		}
	}

	//A Combiner class
	public static class IntSumReducer extends Reducer<Text,CompositeWritable,Text,CompositeWritable> 
	{  
		//Combiner function
		public void reduce(Text key, Iterable<CompositeWritable> values, Context context ) throws IOException, InterruptedException 
		{ 
			CompositeWritable out_max= new CompositeWritable();			//Object for TMAX values
			CompositeWritable out_min= new CompositeWritable();			//Object for TMIN values
			for (CompositeWritable val : values) {						//Traverse through the Iterable values of CompositeWritable
				if ((val.getType()).equals("TMAX"))
				{
					out_max.merge(val);									//Merge all the values for temperature in the MAX object
					out_max.setCount(val);							    //Merge all the values for count in the MAX object
					out_max.setType("TMAX");						    //Set TMAX as type for the object

				}
				if ((val.getType()).equals("TMIN"))
				{
					out_min.merge(val);									//Merge all the values for temperature in the MIN object
					out_min.setCount(val);								//Merge all the values for count in the MIN object
					out_min.setType("TMIN");							//Set TMIN as type for the object
				}

			}
			context.write(key, out_max);                                //Emits (StationID, accumulated temperature, count values along with type) for all TMAX records
			context.write(key, out_min);								//Emits (StationID, accumulated temperature, count values along with type) for all TMIN records
		}
	}



	// A reduce class
	public static class Reduce
	extends Reducer<Text,CompositeWritable,Text,Text> {
		private Text result = new Text();

		// A reduce function
		public void reduce(Text key, Iterable<CompositeWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int count_min =0, count_max =0;								//Initialize the count and the average values for TMIN and TMAX
			Float avg_min =(float) 0, avg_max= (float) 0;
			Float temp_max = (float) 0, temp_min= (float) 0;
			CompositeWritable out_max= new CompositeWritable();			//Object for TMAX
			CompositeWritable out_min= new CompositeWritable();			//Object for TMIN
			for (CompositeWritable val : values) {						//Traverse through the Iterable values of CompositeWritable
				if ((val.getType()).equals("TMAX"))
				{
					out_max.merge(val);									//Merge all the values for temperature in the MAX object
					out_max.setCount(val);								//Merge all the values for count in the MAX object
				}
				if ((val.getType()).equals("TMIN"))
				{
					out_min.merge(val);									//Merge all the values for temperature in the MIN object
					out_min.setCount(val);								//Merge all the values for count in the MIN object
				}

			}
			temp_max = (float) out_max.getValue();								//Retrieve the accumulated max temperature value from its object
			temp_min =  (float) out_min.getValue();								//Retrieve the accumulated min temperature value from its object
			count_min = out_min.getCount();								//Retrieve the accumulated max count value from its object
			count_max = out_max.getCount();								//Retrieve the accumulated min count value from its object

			if(count_min != 0)
				avg_min = temp_min / count_min;                         //Compute mean minimum
			else
	        	avg_min = null;

			if(count_max != 0)
				avg_max = temp_max / count_max; 						//Compute mean maximum
			else
	        	avg_max = null;

			String output = " "+avg_min+" "+avg_max;
			result.set(output);											// Sets the computed means in a Text variable
			context.write(key, result);									//Emits (StationID, (AVG_MIN, AVG_MAX))
		}
	}

	//Main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "no combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CompositeWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
