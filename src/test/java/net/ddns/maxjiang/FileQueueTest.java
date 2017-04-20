package net.ddns.maxjiang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing persistent message queue service
 * 
 * @author Max Jiang
 **/
public class FileQueueTest {

	private static final String QUEUE_NAME = "Test";
	private FileQueueService queue;
	
    @Before
    public void setUp() {
    	queue = spy(FileQueueService.class);
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
        assertTrue(message.getReceiptHandle() != "");
        assertTrue(queue.getVisibilityTimeout(QUEUE_NAME, message.getReceiptHandle()) > System.currentTimeMillis());
        queue.delete(QUEUE_NAME, message.getReceiptHandle());
        
        verify(queue).pull(QUEUE_NAME);
    }

    @Test
    public void deleteTest() {
    	String body = "We are the knights who say Ni";
    	
    	queue.push(QUEUE_NAME, body);
    	Message message = queue.pull(QUEUE_NAME);
        queue.delete(QUEUE_NAME, message.getReceiptHandle());        
        assertTrue(queue.isDeleted(QUEUE_NAME, message.getReceiptHandle()));
        
        verify(queue).delete(any(String.class), any(String.class));
    }
    
	@After
    public void cleanup() {
        queue.shutdown();
    }
}
