package exceptions;

public class DBNameInUse extends DBAppException {

	public DBNameInUse(String name) {
		super(name + " is already used as a name for another table");
		// TODO Auto-generated constructor stub
	}

}
