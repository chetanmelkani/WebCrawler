package com.rd22;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Java class to start a multi-threaded web crawler that scans the apache mailing list
 * for a particular year and stores this data by creating a file for each month for that year
 * 
 * The 2 parameters taht could be configured in the main method are: siteURL and year.
 * 
 * It uses jsoup for parsing and connecting web pages
 * 
 * @author chetanmelkani
 *
 */
public class RunWebCrawler 
{

	private final static Logger LOGGER = Logger.getLogger(RunWebCrawler.class);
	//Store the URLs of web pages for each month for the specified year
	private final static Set<URL> urlMonths = new HashSet<>();
	private final static int MAX_CONSUMER_THREADS = 3;
	private final static int MAX_MONTHS_DATA_THREADS = 3;

	public static void main( String[] args )
	{
		//basic configuration for log4j
		BasicConfigurator.configure();

		//specify the URL to fetch the data from
		String siteURL = "http://mail-archives.apache.org/mod_mbox/maven-users/";
		//specify the year
		int year = 2015;

		//initially fetch the months URL for the specified year and insert it in a set
		try{
			parseURL(siteURL, year);

			//start different threads and pass them URls from this list
			//these threads will create 4 threads, 1 to read URls and the others to download the data
			//when the data download is complete, the data is written to a file

			processDataMonthWise(urlMonths, MAX_MONTHS_DATA_THREADS);

		}
		catch(Exception e){
			LOGGER.error("An exception was raised" + e);
		}
	}

	/**
	 * Parses the URL to fetch and store the 12 months URL in urlMonths set
	 * @param siteURL
	 * @param year
	 * @throws IOException
	 * @throws MalformedURLException
	 */
	private static void parseURL(String siteURL, int year) throws IOException,
			MalformedURLException {
		Document doc = Jsoup.connect(siteURL).get();

		//get all the elements with table tag, check if the year is as specified, if so get href with "/thread"
		Elements elements = doc.select("table");
		for (Element e : elements) {
			if("year".equals(e.attr("class")) && e.select("tr").get(0).text().contains(String.valueOf(year))){
			//if(e.attr("class").equals("year") && e.select("tr").get(0).text().contains(String.valueOf(year))){
				Elements links = e.select("a[href]");
				for (Element el : links) {
					if(el.attr("href").endsWith("/thread")){
						URL monthURL = new URL(siteURL + el.attr("href"));
						urlMonths.add(monthURL);
					}
				}
			}
		}
	}

	/**
	 * Start a thread pool, with a size of 3
	 * Each thread is passed a URL of a month, as this is an independent task
	 * Close the Executor when the job is done.
	 * 
	 * @param urls
	 */
	private static void processDataMonthWise(Set<URL> urls, int maxThreads){

		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		List<Future<Boolean>> futureList = new ArrayList<>();

		for (URL url : urls) {
			String monthDataURL = url.toString();
			String[] arr = monthDataURL.split("/");

			//This controller will further span new threads, it accepts the months URL and a value to create the filename, which will hold the months data
			MonthDataController controller = new MonthDataController(monthDataURL, arr[arr.length -2], MAX_CONSUMER_THREADS);
			Future<Boolean> future = executor.submit(controller);
			futureList.add(future);
		}

		//iterate the future.get(), to know when all the threads have finished
		for (Future<Boolean> future : futureList) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				LOGGER.error("An execption was caught " + e);
			}
		}

		//when all the threads have completed, shutdown the executor
		LOGGER.info("shutting down executor in processDataMonthWise");
		executor.shutdown();
	}

}
