package exceptions;

public class DBTypeMismatch extends DBAppException {

	public DBTypeMismatch(String type, String col) {
		super("You have a type mismatch in this tuple. Expected: " + type + " for " + col);
		// TODO Auto-generated constructor stub
	}

}
