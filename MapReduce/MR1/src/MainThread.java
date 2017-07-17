import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

//MainThread invokes the five different version of the programs
public class MainThread {

	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		int l;
		ArrayList<ArrayList<String>> chunks = new ArrayList<ArrayList<String>>(); //File Chunks data structure
		String FILENAME = args[0];
		//String FILENAME = "C:\\Users\\anpsp\\Desktop\\1912.csv.gz";   //Input file name

		ArrayList<String> fileContent = new ArrayList<String>();   //Contains the input file read into memory

		MainThread m = new MainThread();
		fileContent = m.loadFile(FILENAME, fileContent);  //Loads file from specified input into memory

		int split = (fileContent.size())/4;     //Determines size of each chunk
		chunks = m.chopped(fileContent, split); //Divides input file into chunks

		Sequential s = new Sequential();
		l = 0;
		for(l=0; l<10; l++)
			s.computeAvg(fileContent);         //Computes Average Temperatures sequentially
		s.printValues();					   //Prints certain running times

		ConcurrentNoLocks c = new ConcurrentNoLocks();
		l=0;
		for(l=0; l<10; l++)
			c.computeAvg(chunks);              //Computes Average Temperature concurrently without locks using shared data structure
		c.printValues();					   //Prints certain running times

		CoarseLock co = new CoarseLock();
		l=0;
		for(l=0; l<10; l++)
			co.computeAvg(chunks);			   //Computes Average Temperature concurrently with coarse locks
		co.printValues();					   //Prints certain running times

		FineLock f = new FineLock();
		l=0;
		for(l=0; l<10; l++)
			f.computeAvg(chunks);				//Computes Average Temperature concurrently with fine locks
		f.printValues();						//Prints certain running times

		NoSharing no = new NoSharing();
		l=0;
		for(l=0; l<10; l++)
			no.computeAvg(chunks);				//Computes Average Temperature concurrently without shared data structure
		no.printValues();						//Prints certain running times


	}

	//Load file from Input file into memory
	public ArrayList<String> loadFile(String FILENAME, ArrayList<String> fileContent) throws IOException {
		// TODO Auto-generated method stub
		FileInputStream fin = new FileInputStream(FILENAME);
		GZIPInputStream gzis = new GZIPInputStream(fin);
		InputStreamReader xover = new InputStreamReader(gzis);
		BufferedReader is = new BufferedReader(xover);

		String line;
		while ((line = is.readLine()) != null)                   //Reads every line of input until the end
			fileContent.add(line);							     //Adds each line to data structure
		return fileContent;
	}

	//Divides given Input list into parts each having 'L' max size
	public ArrayList<ArrayList<String>> chopped(ArrayList<String> list, final int L) {
		ArrayList<ArrayList<String>> parts = new ArrayList<ArrayList<String>>();
		final int N = list.size()-1;
		for (int i = 0; i < N; i += L) {
			parts.add(new ArrayList<String>(                                 //Adds a new chunk to the ArrayList that has a maximum size 'L'
					list.subList(i, Math.min(N, i + L)))
					);
		}
		return parts;
	}

}
