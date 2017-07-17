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
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

//Uses coarse lock to implement synchronization for worker threads in a concurrent program
public class CoarseLock {

	static long maxtime = Integer.MIN_VALUE;
	static long mintime = Integer.MAX_VALUE;
	static long sumoftime = 0;
	static long avgtime = 0;
	public static LinkedHashMap<String, Integer> aggregateCounter = new LinkedHashMap<String, Integer>(); //shared data structure		
	public static LinkedHashMap<String, Integer> stationCounter = new LinkedHashMap<String, Integer>();   //shared data structure

	//Computes the average temperatures for those stations having TMAX given an input chunk
	public void computeAvg(ArrayList<ArrayList<String>> chunks) throws InterruptedException {
		// TODO Auto-generated method stub

		long starttime = System.currentTimeMillis();

		String stationID;
		LinkedHashMap<String, Double> output = new LinkedHashMap<String, Double>();
		ArrayList<Thread> threads = new ArrayList<Thread>();

		//Creates four threads giving a chunk of the input file to each of them
		ThreadDemoCoarseLock T1 = new ThreadDemoCoarseLock( "Thread-1", chunks.get(0));
		threads.add(T1);
		T1.start();

		ThreadDemoCoarseLock T2 = new ThreadDemoCoarseLock( "Thread-2", chunks.get(1));
		threads.add(T2);
		T2.start();

		ThreadDemoCoarseLock T3 = new ThreadDemoCoarseLock( "Thread-3", chunks.get(2));
		threads.add(T3);
		T3.start();

		ThreadDemoCoarseLock T4 = new ThreadDemoCoarseLock( "Thread-4", chunks.get(3));
		threads.add(T4);
		T4.start();

		//Waits for each of the threads to finish executing
		for (Thread thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		//Iterates over both the HashMaps to compute the average for each station
		for (Entry<String, Integer> entry : aggregateCounter.entrySet()) {
			stationID = entry.getKey();
			Integer sum = entry.getValue();
			int stationcount = stationCounter.get(stationID);
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
		System.out.println("Maximum time observed for concurrent processing with coarse locks: "+maxtime);
		System.out.println("Minimum time observed for concurrent processing with coarse locks: "+mintime);
		avgtime = sumoftime/10;
		System.out.println("Average time observed for concurrent processing with coarse locks: "+avgtime);
	}

}

//A Thread class that contains the business logic for computing the average TMAX temperatures
class ThreadDemoCoarseLock extends Thread {

	private String threadName;
	private ArrayList<String> split = new ArrayList<String>();


	ThreadDemoCoarseLock( String name, ArrayList<String> chunk) {
		threadName = name;
		split = chunk;
	}

	//Run methods that contains the business logic
	public void run(){

		String stationID;
		String observationType;
		Integer observationValue=0;
		Integer temp=0;
		for(String l : split)
		{
			String[] data = l.split(",");
			stationID = data[0];
			observationType = data[2];
			if(observationType.equals("TMAX"))
				observationValue = Integer.parseInt(data[3]);
			else 
				continue;

			if(CoarseLock.aggregateCounter.containsKey(stationID))
			{
				//Coarse lock on stationCounter data structure
				synchronized(CoarseLock.stationCounter) 				//Coarse lock on the entire data structure - stationCounter
				{
					//Coarse lock on aggregateCounter data structure
					synchronized(CoarseLock.aggregateCounter)           //Course lock on the entire data structure - aggregateCounter
					{
						Integer i = CoarseLock.stationCounter.get(stationID);
						if(i == null)
							i=0;
						i++;													//Increment the station occurrence if it is already present in the list
						CoarseLock.stationCounter.put(stationID, i);			//Station counter is updated 
						temp = CoarseLock.aggregateCounter.get(stationID);
						observationValue += temp;								//Increments the new temperature by the old value
						//fibonacci(17);
						CoarseLock.aggregateCounter.put(stationID, observationValue);		//Updates new value of temperature in the data structures
					}
				}
			}
			else
			{
				//Coarse lock on stationCounter data structure
				synchronized(CoarseLock.stationCounter)						//Coarse lock on the entire data structure - stationCounter
				{
					//Coarse lock on aggregateCounter data structure
					synchronized(CoarseLock.aggregateCounter)				//Course lock on the entire data structure - aggregateCounter
					{
						CoarseLock.aggregateCounter.put(stationID, observationValue);	//Inserts the temperature value into the data structure if new station found
						CoarseLock.stationCounter.put(stationID, 1);					//Assigns one to every new station encountered in the data structure
					}
				}
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



}

