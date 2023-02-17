package com.simplilearn.message.publisher;

import java.time.LocalDateTime;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

@SpringBootApplication
@EnableScheduling
public class MessagePublisherApplication {

	 @Value("${aws.sqs.queue-name}")
	    private String queueName;

	    private AmazonSQS sqsClient;
	    
	    public static void main(String[] args) {
	        SpringApplication.run(MessagePublisherApplication.class, args);
	    }

	    public MessagePublisherApplication() {
	        AWSCredentialsProvider credentialsProvider = InstanceProfileCredentialsProvider.getInstance();
	        sqsClient = AmazonSQSClientBuilder.standard()
	                .withCredentials(credentialsProvider)
	                .withRegion(Regions.AP_NORTHEAST_1)
	                .build();
	    }

	    @Scheduled(fixedDelay = 5000)
	    public void publishMessage() {
	        String message = "Hello, world!"+LocalDateTime.now();
	        SendMessageRequest sendMessageRequest = new SendMessageRequest(queueName, message);
	        sqsClient.sendMessage(sendMessageRequest);
	        System.out.println("Message published: " + message);
	    }
}
