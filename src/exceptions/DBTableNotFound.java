package exceptions;

public class DBTableNotFound extends DBAppException {

	public DBTableNotFound(String message) {
		super(message + " was not found");
		
	}

}
