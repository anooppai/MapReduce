import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

// A custom writable class with three attributes - temperature, type "TMIN" or "TMAX" and count
public class CompositeWritable implements Writable {
		    int val1 = 0;
		    String type ="";
		    int count1 = 0;
		    
		    
		
		    public CompositeWritable() {}

		    public CompositeWritable(int val1, String type, int count1) {
		        this.val1 = val1;
		        this.type = type;
		        this.count1 = count1;
		        
		    }

		    public void readFields(DataInput in) throws IOException {
		        val1 = in.readInt();
		        type = WritableUtils.readString(in);
		        count1 = in.readInt();
		    }

		    public void write(DataOutput out) throws IOException {
		        out.writeInt(val1);
		        WritableUtils.writeString(out, type);
		        out.writeInt(count1);
		    }
		    
		    public String getType(){
		    	return type;
		    }
		    public void setValue(int val1){
		    	this.val1 = val1;
		    }
		    
		    public void incTemp(int val1){
		    	this.val1 += val1;
		    }
		    public int getValue(){
		    	return val1;
		    }
		    
		    public void initCount(int count1){
		    	this.count1 = count1;
		    }
		    public void setType(String s){
		    	this.type = s;
		    }
		    
		    public int getCount(){
		    	return count1;
		    }
		    
		    public void setCount(CompositeWritable other){
		    	this.count1 += other.count1;
		    }
		    
		    public void setCountVal(int count){
		    	this.count1 += count;
		    }

		    public void merge(CompositeWritable other) {
		        this.val1 += other.val1;
		    }
		  
		}