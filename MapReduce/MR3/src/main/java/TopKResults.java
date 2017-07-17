package main.java;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;





import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Class that outputs the top 100 PageRank values in descending order for the given input file
// after ten iterations
public class TopKResults {

	//Mapper that processes every line from the input file and writes 
	//pagename and pagerank values to a TreeMap data structure. Discards the first entry
	//in the data structure each time the size exceeds 100. This works because the property
	//of a TreeMap is that it sorts by key. Every Mapper produces Local TopKResults.
	public static class SOTopTenMapper extends
			Mapper<Object, Text, NullWritable, Text> {
		// TreeMap data structure containing pagerank as key and (pagename and pagerank) as value
		private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

		@Override// A map function call
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//Parse the input to obtain pagerank value and page name
			String line = value.toString();
			String[] tokens = line.split("\t");
			String page = tokens[0];
			String[] links = tokens[1].split(",");
			String pagerank = links[0];
			//Add parsed entries into TreeMap
			repToRecordMap.put(Double.parseDouble(pagerank), new Text(page+"~"+pagerank));

			//Discard first value if size of list exceeds 100
			if (repToRecordMap.size() > 100) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		//At the end of every Mapper, output the values (pagerank + pagename)
		// of the data structure with a single key
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	// A reduce class that collects localtopK results from each mapper and produces 
	// global TopK Results
	public static class SOTopTenReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		// TreeMap data structure containing pagerank as key and (pagename and pagerank) as value
		private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

		@Override// A reduce function call that processes all localtopK values 
		// and produces a global topK value
		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				//for every value sent from the mapper, parse the data and 
				//retreive pagerank and pagename values
				String line = value.toString();
				String[] tokens = line.split("~");
				String page = tokens[0];
				String pagerank = tokens[1];

				//Add parsed entries into TreeMap
				repToRecordMap.put((Double.parseDouble(pagerank)),
						new Text(page+": "+pagerank));

				//Discard first value if size of list exceeds 100
				if (repToRecordMap.size() > 100) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			//Emits all the 100 results in the data structure in a descending order
			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}
}