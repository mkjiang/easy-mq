package net.ddns.maxjiang;

import java.util.HashMap;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * In memory message queue service
 * 
 * @author Max Jiang
 **/
public class InMemoryQueueService implements QueueService {

    private static final int VISIBILITY_TIMEOUT = 30; //seconds
    
    private final Map<String, BlockingQueue<Message>> queues = new HashMap<>();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final Map<String, ScheduledFuture<?>> receivedMessages = new HashMap<>();
   
	private BlockingQueue<Message> getOrCreateQueue(String queueName) {
	    BlockingQueue<Message> queue = queues.get(queueName);

		if (queue == null) {
			synchronized (queues) {
				queue = queues.get(queueName);

				if (queue == null) {
					queue = new LinkedBlockingQueue<Message>();
					queues.put(queueName, queue);
				}
			}
		}

		return queue;
	}
	
    @Override
    public void push(String queueName, String messageBody) {
    	BlockingQueue<Message> queue = getOrCreateQueue(queueName);
    	Message message = new Message(UUID.randomUUID().toString(), messageBody, "");
    	
        try {
			queue.put(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	

    @Override
    public Message pull(String queueName) {
    	BlockingQueue<Message> queue = getOrCreateQueue(queueName);
    	
        //Pull the message off the queue
		Message message = null;
		
		try {
			message = queue.take();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//Add the message back to the queue if not deleted after visibility timeout 
		if (message != null) {
			String receiptHandle = UUID.randomUUID().toString();
			message.setReceiptHandle(receiptHandle);
			final Message msg = message;
	        Runnable task = () -> {
	            try {
	            	queue.put(msg);
					receivedMessages.remove(msg.getId());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        };            
	    	ScheduledFuture<?> future = executor.schedule(task, VISIBILITY_TIMEOUT, TimeUnit.SECONDS);
	
	        //Add the received message to a map along with its scheduled task result
	    	receivedMessages.put(receiptHandle, future);
		}
    	
    	return message;
	}
	
    @Override
    public void delete(String queueName, String receiptHandle) {
    	// Cancel this is effectively deleting the message
        ScheduledFuture<?> future = receivedMessages.remove(receiptHandle);
        
		if (future != null) {
			future.cancel(true);
		}
    }
    
	/**
	 * Retrieve but not remove
	 **/
    public Message peek(String queueName) {
    	BlockingQueue<Message> queue = getOrCreateQueue(queueName);
    	
    	Message message = queue.peek();
    	
		return message;
    }
    
	/**
	 * Shutdown and clear up
	 **/
    public void shutdown() {
    	executor.shutdown();
    	
        try {
        	executor.awaitTermination(10, TimeUnit.SECONDS);
        	executor.shutdownNow();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		receivedMessages.clear();
		queues.clear();
	}
    
	/**
	 * Checks if visibility timeout schedule is running or cancelled
	 **/
    public boolean isTaskRunning (String receiptHandle) {
    	if (receivedMessages.get(receiptHandle) != null) {
    		return true;
    	} else {
    		return false;
    	}
    }
    
    public boolean isScheduleMapEmpty() {
    	return receivedMessages.isEmpty();
    }

}
