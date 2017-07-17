
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class WikiBz2Parser {
	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Input bz2 file required on command line.");
			System.exit(1);
		}

		BufferedReader reader = null;
		try {
			File inputFile = new File(args[0]);
			if (!inputFile.exists() || inputFile.isDirectory() || !inputFile.getName().endsWith(".bz2")) {
				System.out.println("Input File does not exist or not bz2 file: " + args[0]);
				System.exit(1);
			}

			// Configure parser.

			// Parser fills this list with linked page names.


			BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
			reader = new BufferedReader(new InputStreamReader(inputStream));
			String line ="z";
			if (line != null)
				line = reader.readLine();
			else 
				System.exit(0);
			performParse(line);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try { reader.close(); } catch (IOException e) {}
		}
	}

	public static String performParse(String line) throws ParserConfigurationException, SAXException {
		// TODO Auto-generated method stub
		SAXParserFactory spf = SAXParserFactory.newInstance();
		spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		List<String> linkPageNames = new LinkedList<String>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));
		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		if (matcher.find()) {
			// Skip this html file, name contains (~).
			linkPageNames.clear();
			try{
				xmlReader.parse(new InputSource(new StringReader(html)));
				return pageName+"~"+linkPageNames.toString();
			}
			catch (Exception e) {
				
				
			}
		}
		return null;
		

	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	public static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
}