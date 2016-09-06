package com.rd22;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

/**
 * It functions as a controller for single producer and multiple consumers
 * The producer parses the web and inserts links in a blocking queue.
 * The consumers take the data from the queue until, the queue has POISON PILL
 * 
 * There are at most one producer and 3 consumers, multiple consumers help in bringing down the download time of the mails
 * 
 * POISON PILL is used as a communication mechanism between the consumers and the producer
 * @author chetanmelkani
 *
 */
public class MonthDataController implements Callable<Boolean>{
	
	final static Logger LOGGER = Logger.getLogger(MonthDataController.class);
	
	private BlockingQueue<String> queue;
	private String url;
	private String month;
	private int maxConsumerThreads;
	//variable to keep tab on which thread is processing
	private volatile static AtomicInteger atomicCount = new AtomicInteger(0);
	
	//object to let the consumer know that no more data is coming
	public static final String DONE = new String("END OF PRODUCTION");
	
	public MonthDataController(String url, String month, int maxConsumerThreads){
		this.month = month;
		this.url = url;
		this.maxConsumerThreads = maxConsumerThreads;
	}

	/**
	 * The file created does not stores the mail objects in a systematic order
	 * This is done so that parallel processing can be effectively used 
	 */
	@Override
	public Boolean call() throws Exception {
		int count = atomicCount.incrementAndGet();
		LOGGER.info("Starting at call in MonthDataController for thread " + count);
		
		queue = new LinkedBlockingQueue<>();
		List<Future<List<Mail>>> listFuture = new LinkedList<Future<List<Mail>>>();

		ExecutorService executor = Executors.newFixedThreadPool(maxConsumerThreads + 1);
		//start the producer thread
		Crawler crawl = new Crawler(queue, url, count, maxConsumerThreads);
		executor.submit(crawl);
		
		//start 3 consumer threads, this number can be fine tuned based on best performance results
		for(int i=0; i<maxConsumerThreads; i++){
			CreateMailList createMailList = new CreateMailList(queue, count);
			Future<List<Mail>> future = executor.submit(createMailList);
			listFuture.add(future);
		}
		
		//create a file with the name passed to the class
		BufferedWriter bw = new BufferedWriter(new FileWriter(month+".txt"));
		
		//wait for the future to get the List of Mail objects
		//when they are received, write them to a file
		//This is not stored in a systematic order
		for (Future<List<Mail>> future : listFuture) {
			List<Mail> listmail = future.get();
			for (Mail mail : listmail) {
				bw.write(mail.toString());
				bw.write("\n\n");
			}
		}
		
		bw.close();
		executor.shutdown();
		LOGGER.info("Completed controller for thread >>>>" + count);
		return true;
	}

}
