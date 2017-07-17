import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

//Implements no-sharing data structure where each thread have their own data structures
public class NoSharing {

	static long maxtime = Integer.MIN_VALUE;
	static long mintime = Integer.MAX_VALUE;
	static long sumoftime = 0;
	static long avgtime = 0;

	//Computes the average temperatures for those stations having TMAX given an input chunk
	public void computeAvg(ArrayList<ArrayList<String>> chunks) throws InterruptedException {
		// TODO Auto-generated method stub

		long starttime = System.currentTimeMillis();
		String stationID;

		LinkedHashMap<String, Double> output = new LinkedHashMap<String, Double>();
		ArrayList<Thread> threads = new ArrayList<Thread>();

		//Creates four threads giving a chunk of the input file to each of them
		ThreadNoSharing T1 = new ThreadNoSharing( "Thread-1", chunks.get(0));
		T1.start();
		threads.add(T1);

		ThreadNoSharing T2 = new ThreadNoSharing( "Thread-2", chunks.get(1));
		T2.start();
		threads.add(T2);

		ThreadNoSharing T3 = new ThreadNoSharing( "Thread-3", chunks.get(2));
		T3.start();
		threads.add(T3);

		ThreadNoSharing T4 = new ThreadNoSharing( "Thread-4", chunks.get(3));
		T4.start();
		threads.add(T4);

		//Waits for each of the threads to finish executing
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("hello there!");
				e.printStackTrace();
			}
		}

		//Numcount refers to the number of occurrences of each stationID that has TMAX
		LinkedHashMap<String, Integer> numcount1 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> numcount2 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> numcount3 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> numcount4 = new LinkedHashMap<String, Integer>();

		//Sumcount refers to the aggregation of temperature values of each stationID that has TMAX
		LinkedHashMap<String, Integer> sumcount1 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> sumcount2 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> sumcount3 = new LinkedHashMap<String, Integer>();
		LinkedHashMap<String, Integer> sumcount4 = new LinkedHashMap<String, Integer>();

		//Retrieves the numcount for each of the threads
		numcount1 = T1.getNumcount();
		numcount2 = T2.getNumcount();
		numcount3 = T3.getNumcount();
		numcount4 = T4.getNumcount();

		LinkedHashMap<String, Integer> numcount = new LinkedHashMap<String, Integer>(numcount1);

		//Retrives the sumcount for each of the threads
		sumcount1 = T1.getSumcount();
		sumcount2 = T2.getSumcount();
		sumcount3 = T3.getSumcount();
		sumcount4 = T4.getSumcount();

		LinkedHashMap<String, Integer> sumcount = new LinkedHashMap<String, Integer>(sumcount1);

		NoSharing no = new NoSharing();

		//Merges all sumcounts into one map
		sumcount = no.addMaps(sumcount, sumcount2);
		sumcount = no.addMaps(sumcount, sumcount3);
		sumcount = no.addMaps(sumcount, sumcount4);

		//Merges all numcounts into one map
		numcount = no.addMaps(numcount, numcount2);
		numcount = no.addMaps(numcount, numcount3);
		numcount = no.addMaps(numcount, numcount4);

		//Iterates over both the HashMaps to compute the average for each station
		for (Entry<String, Integer> entry : sumcount.entrySet()) {
			stationID = entry.getKey();
			Integer sum = entry.getValue();
			Integer stationcount = numcount.get(stationID);
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
		System.out.println("Maximum time observed for concurrent processing with no sharing data strucuture: "+maxtime);
		System.out.println("Minimum time observed for concurrent processing with no sharing data strucuture: "+mintime);
		avgtime = sumoftime/10;
		System.out.println("Average time observed for concurrent processing with no sharing data strucuture: "+avgtime);
	}

	//Adds two HashMaps and returns the merged map
	public LinkedHashMap<String, Integer> addMaps(LinkedHashMap<String, Integer> m1, LinkedHashMap<String, Integer> m2)
	{
		for (Entry<String, Integer> e : m2.entrySet())
			m1.merge(e.getKey(), e.getValue(), Integer::sum);
		return m1;
	}


}

//A Thread class that contains the business logic for computing the average TMAX temperatures
class ThreadNoSharing extends Thread {

	private String threadName;
	private ArrayList<String> split = new ArrayList<String>();

	LinkedHashMap<String, Integer> aggregateCounter = new LinkedHashMap<String, Integer>(); //Each thread has it's own data structure
	LinkedHashMap<String, Integer> stationCounter = new LinkedHashMap<String, Integer>();	//Each thread has it's own data structure

	ThreadNoSharing( String name, ArrayList<String> chunk) {
		threadName = name;
		split = chunk;
	}

	//Run methods that contains the business logic
	public void run(){

		String stationID;
		String observationType;
		Integer observationValue=0;
		Integer temp=0;
		//Iterates over every single line present in the input file that has been put into memory
		for(String l : split)
		{
			String[] data = l.split(",");
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
				i++;											//Increment the station occurrence if it is already present in the list
				stationCounter.put(stationID, i);				//Station counter is updated
				temp = aggregateCounter.get(stationID);
				observationValue += temp;						//Increments the new temperature by the old value
				//fibonacci(17);
				aggregateCounter.put(stationID, observationValue);	//Updates new value of temperature in the data structure
			}
			else
			{
				aggregateCounter.put(stationID, observationValue);	//Inserts the temperature value into the data structure if new station found
				stationCounter.put(stationID, 1);					//Assigns one to every new station encountered in the data structure
			}
		}
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
		return fibonacci; // 
	}

	//A getter that returns the sumcount HashMap for each thread
	public LinkedHashMap<String, Integer> getSumcount(){
		return aggregateCounter;
	}

	//A getter that returns the numcount HashMap for each thread
	public LinkedHashMap<String, Integer> getNumcount(){
		return stationCounter;
	}

}

