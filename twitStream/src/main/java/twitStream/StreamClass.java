package twitStream;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class StreamClass {

	static int count_1 = 0;
	static String topic = "Trump";
	static String api_key = "1aVnzdKSrxPwTHSO3htci7wjj";
	static String api_secret = "poYuvtU8IlR0ikkAbK7RFot85mewj0aSjjkuA9iHvAvB7Wwe22";
	static String access_token = "276338263-bKICrggnBa3hTVUPgzQtcQpyzMK8cbxJg1vPm4Zv";
	static String access_secret = "q0B6ydthUu1bkXVQKkoFJA9UiNwE1GXO9exYj8ZfyaCDm";
	
	public static void main(String[] args) throws IOException {

		final Properties kafka_prop = new Properties();
		
		kafka_prop.put("bootstrap.servers", "localhost:9092");
		kafka_prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafka_prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafka_prop.put("group.id", "twitGroup");
		kafka_prop.put("retries", 0);
		
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafka_prop);

		ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setOAuthConsumerKey(api_key)
				.setOAuthConsumerSecret(api_secret)
				.setOAuthAccessToken(access_token)
				.setOAuthAccessTokenSecret(access_secret);

		TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

		StatusListener listener = new StatusListener() {
			public void onException(Exception e) {
				e.printStackTrace();
			}

			public void onStatus(Status status) {

				streamTweet(status, producer, topic);
				count_1++;
				if (count_1 % 10 == 0 && count_1 > 1) {
					//System.out.println("Trump # " + count_1);
				}
			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}

			public void onScrubGeo(long arg0, long arg1) {
			}

			public void onStallWarning(StallWarning arg0) {
			}

			public void onTrackLimitationNotice(int arg0) {
			}
		};

		twitterStream.addListener(listener);

		String[] track_terms = { "Trump" };
		twitterStream.filter(track_terms);

	}

	public static void streamTweet(Status status, KafkaProducer<String, String> producer, String topic) {

		producer.send(new ProducerRecord<String, String>(topic, "topic", status.getText()));
		
	}
}