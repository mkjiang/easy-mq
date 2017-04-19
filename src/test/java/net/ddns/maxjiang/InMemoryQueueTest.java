package net.ddns.maxjiang;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing memory based message queue service
 * @author Max Jiang
 **/
public class InMemoryQueueTest {

	private static final int ITEM_COUNT = 5;
	private static final String QUEUE_NAME = "Test";
	private InMemoryQueueService queue;

    @Before
    public void setUp() {
    	queue = spy(InMemoryQueueService.class);
    }
    
    @Test
    public void pushTest() {
    	String body = "We are the knights who say Ni";
    	
	    queue.push(QUEUE_NAME, body);
    	assertEquals(body, queue.peek(QUEUE_NAME).getBody());
    	
    	verify(queue).push(any(String.class), any(String.class));
    }

    @Test
    public void pullTest() {
    	String body = "We are the knights who say Ni";
    	
        queue.push(QUEUE_NAME, body);
        Message message = queue.pull(QUEUE_NAME);
        assertEquals(body, message.getBody());
        assertTrue(queue.isTaskRunning(message.getReceiptHandle()));
        
        queue.delete(QUEUE_NAME, message.getReceiptHandle());
        assertFalse(queue.isTaskRunning(message.getReceiptHandle()));
        
        verify(queue).pull(QUEUE_NAME);
    }

    @Test
    public void deleteTest() {
    	String body = "We are the knights who say Ni";
    	
    	queue.push(QUEUE_NAME, body);
    	Message message = queue.pull(QUEUE_NAME);
        queue.delete(QUEUE_NAME, message.getReceiptHandle());
        assertFalse(queue.isTaskRunning(message.getReceiptHandle()));

        verify(queue).delete(any(String.class), any(String.class));
    }
    
    @Test
    public void threadSafeTest() throws InterruptedException {
    	ExecutorService executorService = Executors.newFixedThreadPool(ITEM_COUNT * 2);
    	CountDownLatch counter = new CountDownLatch(ITEM_COUNT * 2);
    	
        for (int i = 0; i < ITEM_COUNT; i++) {        	
        	
            executorService.submit(() -> {
            	String body;
            	
                for (int j = 0; j< ITEM_COUNT; j++) {

                	if (j%2 == 0) {
                		body = "We are the knights who say Ni";
                	} else {
                		body = "We are the knights who say Ekke Ekke Ekke Ekke Ptang Zoo Boing";
                	}
                	
					queue.push(QUEUE_NAME, body);
				}
                counter.countDown();
            });
          }
        
        for (int i = 0; i < ITEM_COUNT; i++) {
        	
            executorService.submit(() -> {
            	Message message;
            	
                for (int j = 0; j < ITEM_COUNT; j++) {
					message = queue.pull(QUEUE_NAME);
					queue.delete(QUEUE_NAME, message.getReceiptHandle());
                }
                counter.countDown();
            });
        }
        
        counter.await();
        assertNull(queue.peek(QUEUE_NAME));
        
        verify(queue, times(ITEM_COUNT * ITEM_COUNT)).push(any(String.class), any(String.class));
        verify(queue, times(ITEM_COUNT * ITEM_COUNT)).pull(any(String.class));
        verify(queue, times(ITEM_COUNT * ITEM_COUNT)).delete(any(String.class), any(String.class));
        
        executorService.shutdown();
        try {
			executorService.awaitTermination(10, TimeUnit.SECONDS);
	        executorService.shutdownNow();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	@Test
    public void shutdownTest() {
        queue.shutdown();
        assertNull(queue.peek(QUEUE_NAME));
        assertTrue(queue.isScheduleMapEmpty());
        
        verify(queue).shutdown();
    }
    
	@After
    public void cleanup() {
        queue.shutdown();
    }

}
