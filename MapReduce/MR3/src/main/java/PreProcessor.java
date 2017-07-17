package main.java;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

// A preprocessor class that performs the job of pre-processing the given input file by constructing
// an adjacency list and an initial page rank value
public class PreProcessor {

	// A global counter that maintains the total number of nodes
	public static enum PR_COUNTER {
		NUM_NODES
	};

	// A Mapper Class that emits a node with its outgoing links along with an initial pagerank value
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, CompositeWritable>{

		private static Pattern namePattern;
		private static Pattern linkPattern;

		// A data structure that contains the list of outlinks
		List<String> linkPageNames;
		XMLReader xmlReader;
		int n=0;

		// A map function
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			
			//Parsing logic to read from input file to retrieve pagenames
			String line = value.toString();
			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			Matcher matcher = namePattern.matcher(pageName);
			if (matcher.find()) {
				n++;
				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					// Parser fills this list with linked page names.
					xmlReader.parse(new InputSource(new StringReader(html)));
					Text page = new Text();
					page.set(pageName);
					//Increments the global variable with every node encountered
					context.getCounter(PR_COUNTER.NUM_NODES).increment(1);
					context.write(page,new CompositeWritable(-1.0, linkPageNames.size(), linkPageNames));
				} catch (Exception e) {
					//e.printStackTrace();
				}


			}

		}

		// A setup function that declares all the necessary data structures and 
		// defines other parsing configurations
		public void setup(Context context){
			linkPageNames = new LinkedList<String>();
			namePattern = Pattern.compile("^([^~]+)$");
			// Keep only html filenames ending relative paths and not containing tilde (~).
			linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
			SAXParserFactory spf = SAXParserFactory.newInstance();
			try {
				spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			} catch (SAXNotRecognizedException | SAXNotSupportedException
					| ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			SAXParser saxParser;
			try {
				saxParser = spf.newSAXParser();
				xmlReader = saxParser.getXMLReader();
			} catch (ParserConfigurationException | SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// Parser fills this list with linked page names.

			xmlReader.setContentHandler(new Parser.WikiParser(linkPageNames));
		}
	}

	// The main driver function that calls three jobs one after the other.
	// 1. Preprocessing job - constructs the adjacency list from the given input file
	// 2. PageRank job - computes page rank values for every node based on the output
	// 					 received from the preprocessing job
	// 3. TopKResults job - produces top 100 pagerank values after 10 iterations 
	//						of computing pagerank for every node based on the adjacency list
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		//Preprocessing job
		Job job = Job.getInstance(conf, "pre processor");
		job.setJarByClass(PreProcessor.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputValueClass(CompositeWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1] + "/Iteration0"));
		// Proceed to the PageRank job only if the Preprocessing job has finished completion
		if (job.waitForCompletion(true)) {

			//Retrieve value of global counter to pass to next job
			Counter c1 = job.getCounters().findCounter(
					PreProcessor.PR_COUNTER.NUM_NODES);

			Configuration conf1 = new Configuration();
			long delta = 0;
			
			for (int i = 0; i < 10; i++) {
				//PageRank job for 10 iterations
				Job job1 = Job.getInstance(conf1, "page ranker");
				//Sets value of global variables NUM_NODES and DELTA
				job1.getConfiguration().set("COUNTER",
						String.valueOf(c1.getValue()));
				job1.getConfiguration().setLong("DELTA", delta);
				job1.setJarByClass(PageRanker.class);
				job1.setMapperClass(PageRanker.TokenizerMapper2.class);
				job1.setReducerClass(PageRanker.IntSumReducer2.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(Node.class);
				//Input path of current iteration is the output of previous iteration
				FileInputFormat.addInputPath(job1, new Path(otherArgs[otherArgs.length-1] + "/Iteration" + i));
				FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length-1] + "/Iteration" + (i+1)));
				job1.waitForCompletion(true);
				//get updated value of delta so that it can be used in the next iteration
				delta = job1.getCounters()
						.findCounter(PageRanker.PR_COUNTER2.DELTA).getValue();

			}
			//TopKResults job
			Configuration conf2 = new Configuration();
			Job job2 = Job.getInstance(conf2, "Top K Results");
			job2.setJarByClass(TopKResults.class);
			job2.setMapperClass(TopKResults.SOTopTenMapper.class);
			job2.setReducerClass(TopKResults.SOTopTenReducer.class);
			job2.setNumReduceTasks(1);
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			//TopKResults job's input is based on the result of the output of Iteration10 of PageRank
			FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length-1]+"/Iteration10"));
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length-1]+"/TopK"));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}


}
