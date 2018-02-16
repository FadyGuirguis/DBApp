package exceptions;

public class DBInCorrectEntriesNumber extends DBAppException {

	public DBInCorrectEntriesNumber(int args) {
		super("You entered a wrong number of entries. Expected: " + args);
		// TODO Auto-generated constructor stub
	}

}
