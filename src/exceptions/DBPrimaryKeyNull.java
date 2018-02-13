package exceptions;

public class DBPrimaryKeyNull extends DBAppException {

	public DBPrimaryKeyNull() {
		super("The primary key cannot be null");
	}

}
