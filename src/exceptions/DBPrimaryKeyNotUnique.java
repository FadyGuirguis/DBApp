package exceptions;

public class DBPrimaryKeyNotUnique extends DBAppException {

	public DBPrimaryKeyNotUnique() {
		super("The primary key must be unique");
	}

}
