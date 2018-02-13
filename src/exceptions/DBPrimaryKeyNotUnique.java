package exceptions;

public class DBPrimaryKeyNotUnique extends DBAppException {

	public DBPrimaryKeyNotUnique(String message) {
		super("The primary key must be unique");
	}

}
