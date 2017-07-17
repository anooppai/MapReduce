import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

// A Custom writable class that contains two attributes - StationID and year

public class StationYear implements WritableComparable {

	String station = "";
	String year = "";
	
	public StationYear() {super();}
	
	public StationYear(String station, String year){
		this.station = station;
		this.year = year;
	}
	
	public String getStation(){
		return station;
	}
	
	public String getYear(){
		return year;
	}
	
	public void setStation(String station){
		this.station = station;
	}
	
	public void setYear(String year){
		this.year = year;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
        station = WritableUtils.readString(in);
        year = WritableUtils.readString(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, station);
		WritableUtils.writeString(out, year);
		
	}
	
	public int compareTo(Object o) {
		return compareTo((StationYear) o);
	}
	/*
	 * 
	 * Overrides the default compareTo method. Compares by stationID first, if equal, sorts by year in descending order
	 */
	public int compareTo(StationYear pair) {
		// TODO Auto-generated method stub
		int compareValue = this.station.compareTo(pair.getStation());
		if(compareValue == 0)
			compareValue = year.compareTo(pair.getYear());
		return 1*compareValue;
	}
	
}
