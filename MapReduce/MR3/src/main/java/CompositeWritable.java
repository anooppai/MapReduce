package main.java;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


//A CompositeWritable class implementing the Writable interface that contains member fields such as 
// outlinks, pagerank value, count referring to size of outlinks
public class CompositeWritable implements Writable {


	// Member fields
	List<String> links = new LinkedList<String>();
	double pagerank;
	int count;
	public CompositeWritable() {}

	//Constructor
	public CompositeWritable(double pagerank, int count, List<String> links) {
	
		this.pagerank = pagerank;
		this.count = count;
		this.links = links;
	}

	//Read serialization
	 public void readFields(DataInput in) throws IOException{
		 pagerank = in.readDouble();
		 count = in.readInt();
		 for(int i=0; i<count; i++){
			 this.links.add(in.readUTF());
		 }
	 }
	 
	 //Write serialization
	 public void write(DataOutput out) throws IOException{
		 out.writeDouble(pagerank);
		 out.writeInt(links.size());
		 for(String l : links){
			 out.writeUTF(l);
		 }
		 
	 }
	 //Overriding default toString() method to print desired output
	 @Override
	 public String toString() {
		    return pagerank + "," + links.toString();
		}
		 
	 
}