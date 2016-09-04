package com.rd22;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * A consumer which reads URL's from the blocking queue and creates Mail objects
 * Inserts them to a list and return it to the controller
 * @author chetanmelkani
 *
 */
public class CreateMailList implements Callable<List<Mail>> {

	private final static Logger LOGGER = Logger.getLogger(CreateMailList.class);
	private BlockingQueue<String> queue;
	private List<Mail> mailList;
	private int count;

	public CreateMailList(BlockingQueue<String> queue, int count) {
		this.queue = queue;
		mailList = new ArrayList<>();
		this.count = count;
	}

	/**
	 * Multiple consumers will read from this queue
	 * all of them create there mail List and return it, once poison pill is encountered
	 */
	@Override
	public List<Mail> call() throws Exception {
		LOGGER.info("CreateMailList started for thread " + count);
		try{
			while(true){
				String url = queue.take();
				if(url == MonthDataController.DONE){
					LOGGER.info("Finished at CreateMailList for Thread " + count);
					return mailList;
				}
				Mail mail = getMailFromURL(url);
				mailList.add(mail);
			}
		}
		catch(InterruptedException e){
			LOGGER.error("InterruptedException " + e);
			return mailList;
		}
	}

	/**
	 * Create the Mail object for the URL
	 * Return the incomplete Mail object in case any exception is thrown
	 * Log any exception
	 * @param url
	 * @return
	 */
	private Mail getMailFromURL(String url){
		// create the mail object to return
		Mail mail = new Mail();
		Document doc;
		
		try {
			doc = Jsoup.connect(url).get();

			// initialize the mail object from the xml received
			mail.setFrom(doc.select("from").get(0).text());
			mail.setDate(doc.select("date").get(0).text());
			mail.setSubject(doc.select("subject").get(0).text());
			mail.setContent(doc.select("contents").get(0).text());

		} catch (Exception e) {
			LOGGER.error("Exception caught " + e);
		}

		return mail;
	}

}
