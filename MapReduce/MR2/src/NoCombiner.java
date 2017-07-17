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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class NoCombiner {

	// A mapper class
	  public static class TokenizerMapper 
	       extends Mapper<Object, Text, Text, CompositeWritable>{
	      // A Map function
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
	    	if(observationType.equals("TMAX")) {          //Checks if the type of record is TMAX
				temperature = Integer.parseInt(data[3]);  //Parses temperature value from the input record
				type = "TMAX";
				
	    	}
	    	
	    	if(observationType.equals("TMIN")){				//Checks if the type of record is TMIN
				temperature = Integer.parseInt(data[3]);	//Parses temperature value from the input record
				type = "TMIN";
	    	}
	    	if(temperature != 9999)                          
	    	// Emits (stationID, temperature, type and count values) only if a TMIN or TMAX record was found
	    		context.write(station, new CompositeWritable(temperature, type, 1));
	    }
	  }
	  
	  
	  //A Reducer class
	  public static class IntSumReducer 
	       extends Reducer<Text,CompositeWritable,Text,Text> {
	    private Text result = new Text();

	    // A Reduce function
	    public void reduce(Text key, Iterable<CompositeWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int count_min=0, count_max=0;						//Initialize the count and the average values for TMIN and TMAX
	      Float avg_min =(float) 0, avg_max=(float) 0;
	      Float temp_max = (float) 0, temp_min=(float) 0;
	      CompositeWritable out_max= new CompositeWritable();		//Object for TMAX
	      CompositeWritable out_min= new CompositeWritable();		//Object for TMIN
	      for (CompositeWritable val : values) {			//Traverse through the Iterable values of CompositeWritable
	    	if ((val.getType()).equals("TMAX"))
	    	{
	    		out_max.merge(val);                         //Merge all the values for temperature in the MAX object
	    		count_max++;                                //Updates the value of count_max every time MAX is encountered
	    	}
	        if ((val.getType()).equals("TMIN"))
	        {
	        	out_min.merge(val);							//Merge all the values for temperature in the MIN object
	        	count_min++;								//Updates the value of count_min every time MIN is encountered
	        }
	        
	      }
	        temp_max = (float) out_max.getValue();					//Retrieve the accumulated max temperature value from its object
	        temp_min = (float) out_min.getValue();					//Retrieve the accumulated min temperature value from its object
	        
	        if(count_min != 0)
	        	avg_min = temp_min / count_min;             //Compute avg_min
	        else
	        	avg_min = null;
	        
	        if(count_max != 0)
	        	avg_max = temp_max / count_max; 			//Compute avg_max
	        else
	        	avg_max = null;
	   
	        String output = " "+avg_min+" "+avg_max;        
	        //String output = " "+avg_min+" "+avg_max;
	        //Text output = new Text();
	        result.set(output);								// Sets the computed means in a Text variable
	      //result.set(sum);
	      context.write(key, result);						//Emits (StationID, (AVG_MIN, AVG_MAX))
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
	    job.setJarByClass(NoCombiner.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
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
