package project;

public class Dense implements java.io.Serializable {
	
	private String value;
	private Tuple tuple;
	
	public Dense(String value, Tuple tuple) {
		this.value = value;
		this.tuple = tuple;
	}

	public String getValue() {
		return value;
	}

	public Tuple getTuple() {
		return tuple;
	}
	
	
}
