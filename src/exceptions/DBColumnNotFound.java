package exceptions;

public class DBColumnNotFound extends DBAppException{
	
	public DBColumnNotFound(String message) {
		super(message + " is not a column in the this table");
		
	}
}
