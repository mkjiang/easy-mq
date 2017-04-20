package net.ddns.maxjiang;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Persistent message queue service
 * Record format is "visibility_from_time:receipt_handle:id:body" without the quotes
 * 
 * @author Max Jiang
 **/
public class FileQueueService implements QueueService {

    private static final int VISIBILITY_TIMEOUT = 30; //seconds
    private static final String HOME_PATH = "/home/user/";
    private static final String QUEUES_DIR = "sqs";
    
    //Using the Unix POSIX file system for multi-threading implementation
	private void lock(File lock) throws InterruptedException {
		while (!lock.mkdir()) {
			Thread.sleep(50);
		}
	}

	private void unlock(File lock) {
		lock.delete();
	}
	
	private File getMessageFile(String queueName) {
		return new File(HOME_PATH + QUEUES_DIR + "/" + queueName + "/messages");
	}
	
	private File getLockFile(String queueName) {
		return new File(HOME_PATH + QUEUES_DIR + "/" + queueName + "/.lock");
	}
	
	@Override
	public void push(String queueName, String body) {
		File queue = getMessageFile(queueName);
    	File lock = getLockFile(queueName);
    	Boolean append = true;
    	Message message = new Message(UUID.randomUUID().toString(), body, "");
    	
    	if (!queue.exists()) {
    	    append = false;
    	    File parentDir = new File(HOME_PATH + QUEUES_DIR + "/" + queueName);
    	    if (!new File(HOME_PATH + QUEUES_DIR).exists()) {
    	        new File(HOME_PATH + QUEUES_DIR).mkdir();
    	        parentDir.mkdir();
    	    } else {
    	        parentDir.mkdir();
    	    }
    	}
    	
    	try {
    		lock(lock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        try {
        	//Create a new file or append to an existing one depending on "append"
        	PrintWriter pw = new PrintWriter(new FileWriter(queue, append));
        	pw.println(System.currentTimeMillis() + ":" + message.toString());
			pw.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }
	}

	@Override
	public Message pull(String queueName) {
		List<String> records = new ArrayList<String>();
		int topVisibleRecordIndex = -1;
		
		File queue = getMessageFile(queueName);
    	File lock = getLockFile(queueName);
		
		if (!queue.exists()) {
            return null;
		}

        try {
            lock(lock);
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    		
        try {
            //Load all records to a list
            BufferedReader br = new BufferedReader(new FileReader(queue));
            String line;
            int index = 0;
            
            while ((line = br.readLine()) != null) {
            	records.add(line);
            	String[] parts = line.split(":", 2);
            		
            	//Find the top visible record, i.e. top of the queue
            	if (Long.parseLong(parts[0]) <= System.currentTimeMillis()) {
            		topVisibleRecordIndex = index;
            	}
            	index++;
            }
            br.close();
            	
            if (topVisibleRecordIndex != -1) {            
    	        //Update visibility timeout and receipt handle
    	    	String[] parts = records.get(topVisibleRecordIndex).split(":", 3);
    	    	records.set(topVisibleRecordIndex, (System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(VISIBILITY_TIMEOUT)) + ":" + UUID.randomUUID().toString() + ":" + parts[2]);
    	    	
    	       //Overwrite the file
           	    PrintWriter pw = new PrintWriter(new FileWriter(queue));
           	    for (String record : records) {
           		   pw.println(record);
           	    }
                pw.close();
            }
        	
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }

        if (topVisibleRecordIndex == -1) {
            return null;
        }
        
        String[] parts = records.get(topVisibleRecordIndex).split(":", 4);
		
        return new Message(parts[2], parts[3], parts[1]);
	}

	@Override
	public void delete(String queueName, String receiptHandle) {
		List<String> records = new ArrayList<String>();
		
		File queue = getMessageFile(queueName);
    	File lock = getLockFile(queueName);
    	
		if (!queue.exists()) {
            return;
		}
		
        try {
        	lock(lock);
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    	
        try {
    		//Load all records to a list
            BufferedReader br = new BufferedReader(new FileReader(queue));
            String line;

           	while ((line = br.readLine()) != null) {
           		String[] parts = line.split(":", 3);
           		//skip the one needs to be deleted
           		if (parts[1].equals(receiptHandle)) {
           			continue;
           		}
           		records.add(line);
           	}
           	br.close();

    		//Overwrite the file
           	PrintWriter pw = new PrintWriter(new FileWriter(queue));
           	for (String record : records) {
           		pw.println(record);
           	}
   			pw.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }
	}

	/**
	 * Retrieve but not remove
	 **/
    public Message peek(String queueName) {
		String topVisibleRecord = null;
		
        File queue = getMessageFile(queueName);
		File lock = getLockFile(queueName);
		
    	try {
    		lock(lock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
    		//Load all records to a list
        	BufferedReader br = new BufferedReader(new FileReader(queue));
            String line;
            
        	while ((line = br.readLine()) != null) {
        		String[] parts = line.split(":", 2);
        		
        		//Find the top visible record, i.e. top of the queue
        		if (Long.parseLong(parts[0]) <= System.currentTimeMillis()) {
        			topVisibleRecord = line;
        		}
        	}
        	br.close();	
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }
        
        if (topVisibleRecord == null) {
            return null;
        }
        
        String[] parts = topVisibleRecord.split(":", 4);
            
		return new Message(parts[2], parts[3], parts[1]);
    }
    
	/**
	 * Retrieve a message's visibility timeout using its receipt handle
	 **/
    public long getVisibilityTimeout(String queueName, String receiptHandle) {
        File queue = getMessageFile(queueName);
    	long visibilityTimeout = 0;
		
		File lock = getLockFile(queueName);
		
    	try {
    		lock(lock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
    		//Load all records to a list
        	BufferedReader br = new BufferedReader(new FileReader(queue));
            String line;
            
        	while ((line = br.readLine()) != null) {
        		String[] parts = line.split(":", 3);
        		
        		//Find the top visible record, i.e. top of the queue
        		if (parts[1].equals(receiptHandle)) {
        			visibilityTimeout = Long.parseLong(parts[0]);
        		}
        	}
        	br.close();	
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }
        
		return visibilityTimeout;
    }
    
	/**
	 * Check if a message has been deleted (for testing purposes)
	 **/
    public boolean isDeleted(String queueName, String receiptHandle) {
        File queue = getMessageFile(queueName);
    	boolean isDeleted = true;
		
		File lock = getLockFile(queueName);
    	
    	try {
    		lock(lock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
    		//Load all records to a list
        	BufferedReader br = new BufferedReader(new FileReader(queue));
            String line;
            
        	while ((line = br.readLine()) != null) {
        		String[] parts = line.split(":", 2);
        		
        		//Find the top visible record, i.e. top of the queue
        		if (parts[1].equals(receiptHandle)) {
        			isDeleted = false;
        		}
        	}
        	br.close();	
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
        	unlock(lock);
        }
        
        return isDeleted;
    }

	/**
	 * Remove the entire queues directory QUEUES_DIR
	 **/
	public void shutdown() {    	
    	File lock = new File(HOME_PATH + ".lock");
    	
    	try {
    		lock(lock);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	Path directory = Paths.get(HOME_PATH + QUEUES_DIR);
    	try {
			Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
			   @Override
			   public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			       Files.delete(file);
			       return FileVisitResult.CONTINUE;
			   }

			   @Override
			   public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			       Files.delete(dir);
			       return FileVisitResult.CONTINUE;
			   }
			});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
	    	unlock(lock);			
		}
	}
}
