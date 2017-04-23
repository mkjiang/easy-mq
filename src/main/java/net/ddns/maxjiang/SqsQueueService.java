package net.ddns.maxjiang;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * An adapter for Amazon SQS
 * 
 * @author Max Jiang
 **/
public class SqsQueueService implements QueueService {

	AmazonSQSClient sqsClient = new AmazonSQSClient();
	
	public SqsQueueService(AmazonSQSClient sqsClient) {
		this.sqsClient = sqsClient;
	}
	
	@Override
	public void push(String queueName, String messageBody) {
		String queueUrl;

		try {
			queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
		} catch (QueueDoesNotExistException e){
			queueUrl = sqsClient.createQueue(queueName).getQueueUrl();
		}
		
		sqsClient.sendMessage(queueUrl, messageBody);
	}

	@Override
	public Message pull(String queueUrl) {
		ReceiveMessageResult result = sqsClient.receiveMessage(queueUrl);
		
		String id = result.getMessages().get(0).getMessageId();
		String body = result.getMessages().get(0).getBody();
		String receiptHandle = result.getMessages().get(0).getReceiptHandle();
		
		return  new Message(id, body, receiptHandle);
	}

	@Override
	public void delete(String queueUrl, String receiptHandle) {
		sqsClient.deleteMessage(queueUrl, receiptHandle);
	}
}
