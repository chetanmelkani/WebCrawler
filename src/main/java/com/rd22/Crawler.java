package com.rd22;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * Crawls over the URL passes and fetches all the URL's for that month and stores in the blocking queue
 * When all the URL's are enqueued in the queue, inserts the POISON PILL in the queue, the same number of times as the number of consumer threads
 * @author chetanmelkani
 *
 */
public class Crawler implements Callable<Boolean>{
	
	final static Logger LOGGER = Logger.getLogger(Crawler.class);
	private BlockingQueue<String> queue;
	private String url;
	private int count;
	private int maxConsumerThreads;
	
	public Crawler(BlockingQueue<String> queue, String url, int count, int maxConsumerThreads){
		this.queue = queue;
		this.url = url;
		this.count = count;
		this.maxConsumerThreads = maxConsumerThreads;
	}

	@Override
	public Boolean call() throws Exception {

		try{
			parseMailURLs();

			//POISON PILL being inserted in the queue
			for(int i = 0; i< maxConsumerThreads; i++){
				queue.put(MonthDataController.DONE);
			}
			
			LOGGER.info("Queue completed in Thread-" + count);
		}
		catch(InterruptedException e){
			LOGGER.error("InterruptedException " + e);
		}
		return true;
	}

	/**
	 * Get the individual URL's of mails
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 * @throws InterruptedException
	 */
	private void parseMailURLs() throws IOException,
			UnsupportedEncodingException, InterruptedException {
		Document doc = Jsoup.connect(this.url).get();
		//get all the elements with table tag
		Elements elements = doc.select("table");
		for (Element e : elements) {
			if(e.attr("id").equals("msglist")){
				Elements trElements = e.select("tr");
				for (Element trElement : trElements) {
					Elements hrefElements = trElement.select("a[href]");
					for (Element hrefElement : hrefElements) {
						if(hrefElement.parent().attr("class").equals("subject")){

							String s = this.url.substring(0, this.url.length() - 7) + "/ajax/" 
									+ java.net.URLDecoder.decode(hrefElement.attr("href"), "UTF-8").replace(" ", "+");
							queue.put(s);
						}
					}
				}
			}
		}
	}
	
}
