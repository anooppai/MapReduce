package main.java;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;


// A Node Class that contains fields such as pagerank value, outlinks and a flag indicating 
// if a node made any contribution
public class Node implements Writable {
	//member fields
	double pagerank;
	List<String> links;
	boolean hasContributed;

	public Node() {
	}

	//Constructor
	public Node(double pagerank, List<String> links, boolean hasContributed) {
		super();
		this.pagerank = pagerank;
		this.links = links;
		this.hasContributed = hasContributed;
	}

	//Read serialization
	public void readFields(DataInput in) throws IOException {
		pagerank = in.readDouble();
		links = new ArrayList<String>();
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			this.links.add(in.readUTF());
		}
		hasContributed = in.readBoolean();

	}

	//Write serialization
	public void write(DataOutput out) throws IOException {
		out.writeDouble(pagerank);
		out.writeInt(links.size());
		for (String l : links) {
			out.writeUTF(l);
		}
		out.writeBoolean(hasContributed);

	}

	//Getter for the hasContributed field
	public boolean isHasContributed() {
		return hasContributed;
	}

	//Setter for the hasContributed field
	public void setHasContributed(boolean hasContributed) {
		this.hasContributed = hasContributed;
	}

	//Getter for the links field
	public List<String> getLinks() {
		return links;
	}

	//Getter for the links field
	public void setLinks(List<String> links) {
		this.links = links;
	}

	//Getter for the pagerank field
	public double getPagerank() {
		return pagerank;
	}

	//Ssetter for the links field
	public void setPagerank(double pagerank) {
		this.pagerank = pagerank;
	}

	//Overriding the default toString() method to print the desired result
	@Override
	public String toString() {
		return " " + pagerank + "," + links.toString();
	}

}
