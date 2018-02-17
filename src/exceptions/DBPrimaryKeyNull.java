package exceptions;

public class DBPrimaryKeyNull extends DBAppException {

	public DBPrimaryKeyNull(String key) {
		super(key + " is the primary key and cannot be null");
	}

}
