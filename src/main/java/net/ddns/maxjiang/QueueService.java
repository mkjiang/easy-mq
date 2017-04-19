package net.ddns.maxjiang;

/**
 * A Message Queue service used for sending and receiving {@link Message}
 *
 * @author Max Jiang
 */
public interface QueueService {
		
	/**
	 * Pushes a single message onto a specified queue
	 * 
	 * @param message -  an instance of {@link Message}
	 * 
	 **/
    public void push(String queueName, String messageBody);

	/**
	 * Receives a single message from a specified queue
	 * 
	 * @return an instance of {@link Message}
	 * 
	 **/
    public Message pull(String queueURL);

	/**
	 * Deletes a received message
	 * 
	 * @param message - an instance of {@link Message} 
	 * 
	 **/
    public void delete(String queueUrl, String receiptHandle);
    
	/**
	 * Retrieves, but does not remove, the head of this queue
	 * 
	 * @return an instance of {@link Message}
	 * 
	 **/
    //public void peek(String queueURL);
    
	/**
	 * Shutdown the queue service
	 * 
	 **/
    //public void shutdown();
    
}
