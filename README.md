# WebCrawler

A java multi-threaded web crawler which downloads all the emails from a particular year from the URl http://mail-archives.apache.org/mod_mbox/maven-users/.

The main method is present in RunWebCrawler, which initially reads the 12 URL's for each month and stores it in a Set.
Then a thread poll is created which starts processing these URL's with the help of MonthDataController.

The MonthDataController process a months mails by spawning a consumer thread and 3 producers thread, so that the downloading of the Mail object is done parallely, which inturn increases the performance of the crawler.

It is designed to run concurrently and uses the concepts of Producer Consumers.
Where a Producer called "Crawler" parses the web pages and inserts the relevant URL's to a blocking queue.
And multiple consumers called the CreateMailList will take from this queue and create a List of Mail objects, which are later written to a file.

The Crawler and the CreateMailList communicate with each other using a poison pill, this helps to close the consumer when the producer has stopped producing.

A file is created as soon as the CreateMailList Thread starts returning the List of Mails.
The Mails objects are kept on writing to the file untill all the CreateMailList Threads for a MonthDataController have returned.

Once all the Cunsumers threads have returned, the executer is shutdown.

Limitations:

1. If an exception is thrown while connecting to a URL or parsing some data, it is simply ignored and the incomplete data is saved in the file.

2. If a months has multiple pages for the mails, the crawler picks data from the first page only and ignore the others.

