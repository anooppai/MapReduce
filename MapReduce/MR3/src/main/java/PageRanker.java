package main.java;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

//A Class that computes the pagerank values for every node based on the adjacency list received
//from the input file
public class PageRanker {
	// A Global counter that keeps track the value of Delta for dangling nodes
	public static enum PR_COUNTER2 {
		DELTA
	};

	//A Map class that process every line from the input value and parses to
	//retrieve the pagerank values and the adjacency list for a given node, sending along
	// the outlinks of every node and also its contribution
	public static class TokenizerMapper2 extends
			Mapper<Object, Text, Text, Node> {

		//A map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parses every line from the input
			String line = value.toString().replaceAll("\\]", "")
					.replaceAll("\\[", "").replaceAll(" ", "");
			String[] tokens = line.split("\t");
			String page = tokens[0];   //gets the pagename
			Text pagename = new Text(page);

			String[] links = tokens[1].split(",");
			Configuration conf = context.getConfiguration();
			String counter = conf.get("COUNTER");
			double pagerank = Double.parseDouble(links[0]); //gets the pagerank value
			if (pagerank == -1.0) {
				// Assigns the pagerank to 1/NUM_NODES for first iteration
				pagerank = 1 / (double) Long.parseLong(counter);  
			}

			//Declares a outlink data structure that contains the outlinks for a particular node
			List<String> outlinks = new ArrayList<String>();
			for (int i = 1; i < links.length; i++) {
				String link = links[i];
				outlinks.add(link);
			}

			// If a node is a dangling, increment the value of delta by the pagerank value
			if (outlinks.size() == 0) {
				context.getCounter(PR_COUNTER2.DELTA)
						.increment((long) (pagerank *Math.pow(10, 5)));
			} else {
				// If non-dangling node is found, send along its contributions to the reducer
				Node node = new Node();
				double contributions = pagerank / outlinks.size();
				for (String l : outlinks) {
					node.setPagerank(contributions); 		// Sets new pagerank value to computed contributions
					node.setHasContributed(true);			// Flag used to indicate that the node is a contributor
					ArrayList<String> empty = new ArrayList<String>();
					node.setLinks(empty);

					Text newpage = new Text(l);
					context.write(newpage, node);           //Emits the node along with the computed contribution
				}
			}
			//Send the structure of the node to the reducer without incorporating any contribution
			Node n = new Node(pagerank, outlinks, false);
			context.write(pagename, n);

		}
	}

	// A reducer class that outputs the computed pagerank value for every node
	public static class IntSumReducer2 extends Reducer<Text, Node, Text, Node> {
		private Node linknode;

		// A reduce function that processes the given list of Node Objects that contain contributions 
		// and the node structure
		public void reduce(Text key, Iterable<Node> values, Context context)
				throws IOException, InterruptedException {

			linknode = new Node();
			List<String> initLink = new ArrayList<String>();
			linknode.setLinks(initLink);
			double pagerank = 0.0;

			//Traverses through every Node object sent from the mapper
			for (Node n : values) {
				//If node object did not make a contribution, recover the node structure
				if (!n.isHasContributed()) {
					linknode.setHasContributed(n.isHasContributed());
					linknode.setLinks(n.getLinks());
					linknode.setPagerank(n.getPagerank());
				} else
					//otherwise, aggregate the values of pageranks received
					pagerank += n.getPagerank();
			}

			Configuration conf = context.getConfiguration();
			String counter = conf.get("COUNTER");
			//Retrieve the value of delta from the global counter
			double delta = conf.getLong("DELTA", 0)/Math.pow(10, 5);

			//Formula to compute the new pagerank value
			pagerank = (0.15 / Long.parseLong(counter)) + 0.85
					* ((delta / Long.parseLong(counter)) + pagerank);
			//Set computed pagerank value
			linknode.setPagerank(pagerank);
			//Emit pagename and the node structure containing page rank values and it's outgoing links
			context.write(key, linknode);
		}

	}
}