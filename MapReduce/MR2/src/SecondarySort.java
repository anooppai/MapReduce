import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SecondarySort {

	  public static class TokenizerMapper 
	       extends Mapper<Object, Text, StationYear, CompositeWritable>{
	      
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	Text station = new Text();
	 	    String observationType;
	 		String year;
	 		String type = null;
	 		String stationID;
	    	int temperature= 9999;
	    	String line = value.toString();
	    	String[] data = line.split(",");
	    	stationID = data[0];
	    	year = data[1];
	    	year = year.substring(0, 4); //Retrieves year from the record
	    	station.set(stationID);
	    	observationType = data[2];
	    	if(observationType.equals("TMAX")) {				//Checks if the type of record is TMAX
				temperature = Integer.parseInt(data[3]);		//Parses temperature value from the input record
				type = "TMAX";
				
	    	}
	    	
	    	if(observationType.equals("TMIN")){					//Checks if the type of record is TMIN
				temperature = Integer.parseInt(data[3]);		//Parses temperature value from the input record
				type = "TMIN";
	    	}
	    	if(temperature != 9999)
	    		// In this case, year attribute is added to the natural key to turn it into a composite key 
	    		// as we want the reducer's output to be sorted by year.
	    		// Emits ((Station, year) , temperature, type and count values) only if a TMIN or TMAX record was found
	    		context.write(new StationYear(stationID, year), new CompositeWritable(temperature, type, 1));
	    }
	  }
	  
	  // A key comparator
	  public static class KeyComparator extends WritableComparator {
		    protected KeyComparator() {
		      super(StationYear.class, true);
		    }
		    @Override
		    public int compare(WritableComparable w1, WritableComparable w2) {
		    	StationYear ip1 = (StationYear) w1;
		    	StationYear ip2 = (StationYear) w2;
		     // Sorts in increasing order of Station first
		      int cmp = ip1.getStation().compareTo(ip2.getStation());
		      if (cmp != 0) {
		        return cmp;
		      }
		      // If StationIDs are equal, sorts in ascending order of year next
		      return ip1.getYear().compareTo(ip2.getYear()); //reverse
		    }
		  }
	  
	  // A grouping comparator
	  // This comparator decides which keys are grouped together for a single reduce call Reducer.reduce() function
	  // In the case of this example, with the help of a grouping comparator,
	  // an entry such as ((AGE00135039, 1884, V1) (AGE00135039, 1885, V2) ends up in the same reduce call and are 
	 // considered identical. 
	  public static class GroupComparator extends WritableComparator {
		    protected GroupComparator() {
		      super(StationYear.class, true);
		    }
		    @Override
		    public int compare(WritableComparable w1, WritableComparable w2) {
		      StationYear ip1 = (StationYear) w1;
		      StationYear ip2 = (StationYear) w2;
		      //Sorts in increasing order of StationID
		      return ip1.getStation().compareTo(ip2.getStation());
		    }
		  }
	  
	  // A Partitioner class
	  // This class decides which mapper's output goes to which reducer based on the mapper's output key.
	  // A custom partitioner ensures that all data with the same key(StationID) is sent to the same reducer
	  public static class SecondarySortBasicPartitioner extends
	  	Partitioner<StationYear, CompositeWritable> {

		@Override
		// A Partitioner function
		public int getPartition(StationYear key, CompositeWritable value,
				int numReduceTasks) {

			//Distributes the records to the reducers by station
			return (Math.abs(key.getStation().hashCode()) % numReduceTasks);
		}
	}
	  
	  
	  // A Reduce class :
	  // By using a grouping comparator to perform secondary sort, we can group data within a reduce task. 
	  // Thus, secondary sort is used to sort the value attributes (year), while the key is inherently sorted by the mappers
	  // before starting reducers.
	  // Therefore, in general, there is a single reduce call per (AGE00135039, *).
	  //an entry such as ((AGE00135039, 1886, V1) (AGE00135039, 1881, V2) (AGE00135039, 1883, V3) would appear in 
	  //Reduce’s input list as (AGE00135039, (1881, V2), (1883, V3) (1886, V1)) as the years have been sorted in increasing order.
	  public static class IntSumReducer 
	       extends Reducer<StationYear,CompositeWritable,Text,Text> {

	    public void reduce(StationYear s, Iterable<CompositeWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int count_min=0, count_max=0;										//Initialize the count and the average values for TMIN and TMAX
	      Float avg_min =(float) 0, avg_max= (float) 0;
	      Float temp_max = (float) 0, temp_min=(float) 0;
	      String prevyear = s.getYear();
	      ArrayList<String> partial = new ArrayList<String>();             //Declare an ArrayList data structure to add the computed average values for a single station
	      CompositeWritable out_max= new CompositeWritable();			   //Object for TMAX
	      CompositeWritable out_min= new CompositeWritable();			   //Object for TMIN
	      
	      for (CompositeWritable val : values) {							//Traverse through the Iterable values of CompositeWritable
	        //max_temp += val.get();
	    	if (!prevyear.equals(s.getYear()))                             //Checks if the current year is the same as previous year
	    	{
	    		temp_max = (float) out_max.getValue();							// Retrieve the accumulated max temperature value from its object
	 	        temp_min = (float) out_min.getValue();							// Retrieve the accumulated max temperature value from its object
	 	        
	 	        if(count_min != 0)
	 	        	avg_min = temp_min / count_min;  					//Compute average min
	 	        else 
	 	        	avg_min = null;
	 	        
	 	        if(count_max != 0)		
	 	        	avg_max = temp_max / count_max; 					//Compute average max
	 	        else
	 	        	avg_max = null;
	    		String output = "("+prevyear+","+avg_min+","+avg_max+")";   //Wraps the year and average min and max values in a string
	    		partial.add(output);									// Append to list
	    		avg_min = (float) 0;
	    		avg_max = (float) 0;
	    		count_min = 0;                                          // Flush the values of all average variables to zero
	    		count_max = 0;
	    		temp_min = (float) 0;
	    		temp_max = (float) 0;
	    		out_max.setValue(0);
	    		out_min.setValue(0);
	    		
	    	}
	    	prevyear = s.getYear();                                     // Updates the variable of prev year
	    	if ((val.getType()).equals("TMAX"))
	    	{
	    		out_max.merge(val);										//Merge all the values for temperature in the MAX object
	    		count_max++;											//Updates the value of count for MAX records
	    	}
	        if ((val.getType()).equals("TMIN"))
	        {
	        	out_min.merge(val);										//Merge all the values for temperature in the MIN object
	        	count_min++;											//Updates the value of count for MIN records
	        }
	        
	      }
	        temp_max = (float) out_max.getValue();								//Retrieve the accumulated max temperature value from its object
	        temp_min = (float) out_min.getValue();								//Retrieve the accumulated min temperature value from its object
	        
	        if(count_min != 0)
	        	avg_min = temp_min / count_min;
	        else
	        	avg_min = null;
	        
	        if(count_max != 0)
	        	avg_max = temp_max / count_max;                          //Compute averages
	        else
	        	avg_max = null;
	        String output = "("+prevyear+","+avg_min+","+avg_max+")";
    		partial.add(output);                                         //Append year and computes values to list
	   
    		//Emits (StationID, (Year, Average_min and Average_max)
    		context.write(new Text(s.getStation()), new Text(partial.toString()));  
	    }
	  }

	  
	  // A main function
	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = Job.getInstance(conf, "no combiner");
	    job.setJarByClass(SecondarySort.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	    job.setPartitionerClass(SecondarySortBasicPartitioner.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(StationYear.class);
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
