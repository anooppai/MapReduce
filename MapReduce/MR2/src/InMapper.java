import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class InMapper {

	//A Mapper class
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, CompositeWritable>{


		HashMap<String, CompositeWritable> map1; //A HashMap data structure that contains all those key-pair values for 'TMAX'
		HashMap<String, CompositeWritable> map2; //A HashMap data structure that contains all those key-pair values for 'TMIN'

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			//A composite class object
			CompositeWritable comp = new CompositeWritable();
			Text station = new Text();
			String observationType;
			String type = null;
			String stationID;
			int temperature= 9999; 
			String line = value.toString();
			String[] data = line.split(","); //Splitting the input on ","
			stationID = data[0];
			station.set(stationID);
			observationType = data[2];
			if(observationType.equalsIgnoreCase("TMAX")) {
				temperature = Integer.parseInt(data[3]);   //Temperature parsed from the input
				type = "TMAX";
				comp.setValue(temperature);      //Sets the object values to temperature, type and counts
				comp.setType("TMAX");
				comp.initCount(1);
				if(map1.containsKey(stationID)){  //Checks if the map already contains the StationID
					CompositeWritable co1;
					co1 = map1.get(stationID);   //Get the CompositeWritable object for a particular Station
					co1.incTemp(temperature);    //Update the temperature, count and type values in the object
					co1.setCountVal(1);
					co1.setType(type);
				}
				else
					map1.put(stationID, comp);   //Make an entry into the HashMap if the stationID doesn't already exist

			}

			if(observationType.equalsIgnoreCase("TMIN")){
				temperature = Integer.parseInt(data[3]);     //Temperature parsed from the input
				type = "TMIN";
				comp.setValue(temperature);					 //Sets the object values to temperature, type and counts
				comp.setType("TMIN");
				comp.initCount(1);
				if(map2.containsKey(stationID)){				//Checks if the map already contains the StationID
					CompositeWritable co2;
					co2 = map2.get(stationID);					//Get the CompositeWritable object for a particular Station
					co2.incTemp(temperature);					//Update the temperature, count and type values in the object
					co2.setCountVal(1);
					co2.setType(type);
				}
				else
					map2.put(stationID, comp);                //Make an entry into the HashMap if the stationID doesn't already exist

			}

		}


		// A setup() function that defines the HashMaps map1 and map2 at the start of the Mapper phase
		public void setup(Context context){
			if(null == map1) //lazy loading
				map1 = new HashMap<String, CompositeWritable>();
			if(null == map2)
				map2 = new HashMap<String, CompositeWritable>();
		}


		// A cleanup() function that emits the accumulated values of temperature and count along 
		// with the type for every station at the end of the mapper phase
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (String key : map1.keySet()) {
				Text newkey = new Text();
				newkey.set(key);
				//Emits (stationID, accumulated temperature, type and count for TMAX
				context.write(newkey, map1.get(key));
			}
			for (String key : map2.keySet()) {
				Text newkey = new Text();
				newkey.set(key);
				//Emits (stationID, accumulated temperature, type and count for TMIN
				context.write(newkey, map2.get(key));
			}
		}

	}



	// A Reduce class
	public static class Reduce
	extends Reducer<Text,CompositeWritable,Text,Text> {
		private Text result = new Text();   

		public void reduce(Text key, Iterable<CompositeWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			int count_min =0, count_max =0;   //Initialize the count and the average values for TMIN and TMAX
			Float avg_min =(float)0, avg_max=(float) 0;
			Float temp_max = (float)0, temp_min=(float) 0;
			CompositeWritable out_max= new CompositeWritable(); //Object for TMAX
			CompositeWritable out_min= new CompositeWritable(); //Object for TMIN
			for (CompositeWritable val : values) {  //Traverse through the Iterable values of CompositeWritable
				if ((val.getType()).equals("TMAX"))
				{
					out_max.merge(val);                //Merge all the values for temperature in the MAX object
					count_max = val.getCount();        //Get the value of count from the traversed object
					out_max.setCountVal(count_max);    //Updates the count value in the MAX object
				}
				if ((val.getType()).equals("TMIN"))
				{
					out_min.merge(val);					 //Merge all the values for temperature in the MIN object
					count_min = val.getCount();			//Get the value of count from the traversed object
					out_min.setCountVal(count_min);		//Updates the count value in the MIN object
				}

			}
			temp_max = (float) out_max.getValue();               //Retrieve the accumulated max temperature value from its object
			temp_min = (float) out_min.getValue();				//Retrieve the accumulated min temperature value from its object
			count_min = out_min.getCount();				//Retrieve the accumulated min count value from its object
			count_max = out_max.getCount();				//Retrieve the accumulated max count value from its object

			if(count_min != 0)
				avg_min = temp_min / count_min;         //Computes mean minimum
			else
	        	avg_min = null;

			if(count_max != 0)
				avg_max = temp_max / count_max; 		//Computes mean maximum
			else
	        	avg_max = null;

			String output = " "+avg_min+" "+avg_max;
			result.set(output);                         // Sets the computed means in a Text variable
			context.write(key, result);                 //Emits (StationID, (AVG_MIN, AVG_MAX))
		}
	}

	// Main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "In Mapper");
		job.setJarByClass(InMapper.class);
		job.setMapperClass(TokenizerMapper.class);
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
