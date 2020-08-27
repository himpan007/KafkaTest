package org.himpan.KafkaTest;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.logging.LogFactory;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

String consumerKey = "46qz5uA2yp6G2iMe2xJHFLWtp";
String consumerSecret = "B9e6m70YvA51M0k77KXYIHm1ZLGYYR97b4yqD97MK7tUqbuSGy";
String token = "3187788512-Dob4w0JU1AzZvpEwN0zhG8eA9pEhNc3TLsvTFmA";
String secret = "UfOFjQwFF1LkRJZAHuMz2sDNUCNXxi3TmDjXRYUwTeUOx";

	//public TwitterProducer() {}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	new TwitterProducer().run();
		
	}
	
public void run() {
	
	logger.info("enter here");

	/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
   //AWS REGION :oregon 
	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
	// create twitter client
	Client client = createTwitterClient(msgQueue);
			client.connect();
			
			while (!client.isDone()) {
				  String msg = null;
				try {
					msg = msgQueue.poll(5, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					client.stop();
				}
				  if(msg != null) {
					 logger.info(msg);
				  }
				  logger.info("End of information");
				
				}
}

public Client createTwitterClient(BlockingQueue<String> msgQueue ) {
	
	
	/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
	Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
	
	StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
	// Optional: set up some followings and track terms
	
	//List<Long> followings = Lists.newArrayList(1234L, 566788L);
	List<String> terms = Lists.newArrayList("Government");
	//hosebirdEndpoint.followings(followings);
	hosebirdEndpoint.trackTerms(terms);

	// These secrets should be read from a config file
	Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
	
	ClientBuilder builder = new ClientBuilder()
			  .name("Hosebird-01")                              // optional: mainly for the logs
			  .hosts(hosebirdHosts)
			  .authentication(hosebirdAuth)
			  .endpoint(hosebirdEndpoint)
			  .processor(new StringDelimitedProcessor(msgQueue));
			 // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

			Client hosebirdClient = builder.build();
			// Attempts to establish a connection.
			hosebirdClient.connect();
			return hosebirdClient;
	
}
}
