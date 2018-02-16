package exceptions;

public class DBUnsupportedType extends DBAppException {

	public DBUnsupportedType(String type) {
		super("This type is not supported: " + type);
		// TODO Auto-generated constructor stub
	}

}
