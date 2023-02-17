package com.simplilearn.message.consumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

@SpringBootApplication
@EnableScheduling
@EnableJpaRepositories(basePackages = "com.simplilearn.message.consumer")
@ComponentScan
public class MessageConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MessageConsumerApplication.class, args);
	}

	@Component
	public class MessageConsumer {

		@Value("${aws.sqs.queue-name}")
		private String queueName;

		@Value("${spring.datasource.url}")
		private String dbUrl;

		@Value("${spring.datasource.username}")
		private String dbUsername;

		@Value("${spring.datasource.password}")
		private String dbPassword;
		
		private AmazonSQS sqsClient;

		private AmazonRDS rdsClient;

		public MessageConsumer() {
			AWSCredentialsProvider credentialsProvider = InstanceProfileCredentialsProvider.getInstance();
			sqsClient = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider)
					.withRegion(Regions.AP_NORTHEAST_1).build();
			rdsClient = AmazonRDSClientBuilder.standard().withCredentials(credentialsProvider)
					.withRegion(Regions.AP_NORTHEAST_1).build();
		}

		@Scheduled(fixedDelay = 1000)
		public void receiveMessage() throws SQLException {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
					.withQueueUrl(sqsClient.getQueueUrl(queueName).getQueueUrl()).withMaxNumberOfMessages(1);
			List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
			for (Message message : messages) {
				String body = message.getBody();
				Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
				PreparedStatement ps = conn.prepareStatement("INSERT INTO messages(message) VALUES (?)");
				ps.setString(1, body);
				ps.execute();
				sqsClient.deleteMessage(sqsClient.getQueueUrl(queueName).getQueueUrl(), message.getReceiptHandle());
			}
		}

	}


}
