package net.ddns.maxjiang;

/**
 * A message contains an id, a message body and a receipt handle (an identifier associated with the act of receiving the message) 
 * 
 * @author Max Jiang
 **/
public class Message {
	
	private String id;
    private String body;
    private String receiptHandle;

	public Message(String id, String body, String receiptHandle) {
	    this.id = id;
		this.body = body;
		this.receiptHandle = receiptHandle;
	}

    public String getId() {
    	return id;
    }
    
    public String getBody() {
        return body;
    }
    
    public String getReceiptHandle() {
        return receiptHandle;
    }
    
    public void setId(String id) {
    	this.id = id;
    }
    
    public void setBody(String body) {
		this.body = body;
	}

	public void setReceiptHandle(String receiptHandle) {
		this.receiptHandle = receiptHandle;
	}
	
	//Keep it simple for now, could use GSON to make it nicer
    public String toString(){
    	return getReceiptHandle() + ":" + getId() + ":" + getBody();
    }

}