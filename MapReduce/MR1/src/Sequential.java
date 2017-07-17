import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

//Performs sequentially to compute the average TMAX temperatures
public class Sequential {

	static long maxtime = Integer.MIN_VALUE;        //Default value of max running time
	static long mintime = Integer.MAX_VALUE;		//Default value of min running time
	static long sumoftime = 0;						//Default value of aggregation of all running times
	static long avgtime = 0;						//Default value of average time



	//Computes the average temperatures for those stations having 'TMAX' in their fields
	public void computeAvg(ArrayList<String> fileContent) {

		long starttime = System.currentTimeMillis();

		String stationID;
		LinkedHashMap<String, Integer> stationCounter = new LinkedHashMap<String, Integer>(); //Data structure that contains the number of occurrences of each stationID that has TMAX
		LinkedHashMap<String, Integer> aggregateCounter = new LinkedHashMap<String, Integer>(); //Data structure that contains the aggregation of temperatures for each stationID that has TMAX
		LinkedHashMap<String, Double> output = new LinkedHashMap<String, Double>(); //Final data structure that contains the average of each stationID
		String observationType;
		Integer observationValue=0;
		Integer temp=0;

		//Iterates over every single line present in the input file that has been put into memory
		for(String l : fileContent)
		{
			String[] data = l.split(",");    //splits the line by a ','
			stationID = data[0];
			observationType = data[2];
			if(observationType.equals("TMAX"))
				observationValue = Integer.parseInt(data[3]);
			else 
				continue;
			if(aggregateCounter.containsKey(stationID))
			{
				Integer i = stationCounter.get(stationID);
				if(i == null)
					i=0;
				i++;                                                  //Increment the station occurrence if it is already present in the list
				stationCounter.put(stationID, i);					  //Station counter is updated 
				temp = aggregateCounter.get(stationID);							  
				observationValue += temp;							  //Increments the new temperature by the old value
				//fibonacci(17);
				aggregateCounter.put(stationID, observationValue);				  //Updates new value of temperature in the data structure
			}
			else
			{
				aggregateCounter.put(stationID, observationValue);                //Inserts the temperature value into the data structure if new station found
				stationCounter.put(stationID, 1);					  //Assigns one to every new station encountered in the data structure
			}
		}

		//Iterates over both the HashMaps to compute the average for each station
		for (Entry<String, Integer> entry : aggregateCounter.entrySet()) {
			stationID = entry.getKey();
			Integer sum = entry.getValue();
			Integer stationcount = stationCounter.get(stationID);
			double avg = (double) sum/stationcount;
			output.put(stationID, avg);		
		}

		//Computes total time, max time and min time taken by the process
		long endtime = System.currentTimeMillis();

		
		long totaltime = endtime - starttime;
		if(totaltime > maxtime)
			maxtime = totaltime;

		if(totaltime < mintime)
			mintime = totaltime;

		sumoftime += totaltime;

	}

	//Prints Maxtime, mintime and average of the running times observed
	public void printValues()
	{
		System.out.println("Maximum time observed for sequential processing: "+maxtime);
		System.out.println("Minimum time observed for sequential processing: "+mintime);
		avgtime = sumoftime/10;
		System.out.println("Average time observed for sequential processing: "+avgtime);
	}

	//Computes Fibonacci number for a given number
	public static int fibonacci(int number) {
		if (number == 1 || number == 2) {
			return 1;
		}
		int fibo1 = 1, fibo2 = 1, fibonacci = 1;
		for (int i = 3; i <= number; i++) {
			fibonacci = fibo1 + fibo2; // Fibonacci number is sum of previous two Fibonacci number
			fibo1 = fibo2;
			fibo2 = fibonacci;

		}
		return fibonacci; 
	}
}

