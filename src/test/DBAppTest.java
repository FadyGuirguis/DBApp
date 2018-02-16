package test;

import java.util.Hashtable;

import exceptions.DBAppException;
import exceptions.DBNameInUse;
import project.DBApp;

public class DBAppTest {
	
	public static void main(String[] args)
	{
		//creating the application and initializing it
		DBApp ourApp = new DBApp();
		ourApp.init();

		//creating tables
		//I'm using the same hashtable table htblColNameType so make sure u clear it before creating every table
		String strTableName = null;

		try {
			strTableName = "Student";
			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			ourApp.createTable(strTableName, "id", htblColNameType);
			
			String strTableName2 = "Employee";
			htblColNameType.clear();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("salary", "java.lang.Double");
			ourApp.createTable(strTableName2, "id", htblColNameType);
			
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		
		


		
		
		//inserting some tuples 
		//I'm using the same hashtable table htblColNameValue so make sure u clear it before inserting each time
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("id", new Integer(4898));
		htblColNameValue.put("name", new String("Peter"));
		htblColNameValue.put("gpa", new Double(0.8));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(5674));
		htblColNameValue.put("name", new String("Fady"));
		htblColNameValue.put("gpa", new Double(0.7));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(8475));
		htblColNameValue.put("name", new String("Zeyad"));
		htblColNameValue.put("gpa", new Double(0.69));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(1234));
		htblColNameValue.put("name", new String("Mariz"));
		htblColNameValue.put("gpa", new Double(0.87));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(9756));
		htblColNameValue.put("name", new String("Bishoy"));
		htblColNameValue.put("gpa", new Double(0.92));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

		htblColNameValue.clear();
		htblColNameValue.put("id", new Integer(12464));
		htblColNameValue.put("name", new String("Rony"));
		htblColNameValue.put("gpa", new Double(0.95));
		ourApp.insertIntoTable(strTableName, htblColNameValue);

	}

}
