package test;

import java.awt.List;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;

import exceptions.DBAppException;
import exceptions.DBNameInUse;
import project.Brin;
import project.DBApp;
import project.Dense;
import project.Tuple;

public class DBAppTest {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException
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
			Hashtable<String, String> htblColNameType2 = new Hashtable<String, String>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			ourApp.createTable(strTableName, "id", htblColNameType);
			
			String strTableName2 = "Employee";
			//htblColNameType.clear();
			htblColNameType2.put("id", "java.lang.Integer");
			htblColNameType2.put("name", "java.lang.String");
			htblColNameType2.put("salary", "java.lang.Double");
			ourApp.createTable(strTableName2, "id", htblColNameType2);
			
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
//
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
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(3600));
			htblColNameValue.put("name", new String("Daniel"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
//			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(780));
			htblColNameValue.put("name", new String("Maggie"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
//			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(6798));
			htblColNameValue.put("name", new String("Mostafa"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
////			for (int i = 1; i <=3; i++)
////			{
////				ObjectInputStream in;
////				ArrayList<Tuple> results = null;
////				System.out.println("page" + i);
////				try {
////					String pagePath = "src/DB2App/Student Table/Page " + i + ".ser";
////					FileInputStream fileIn = new FileInputStream(pagePath);
////					in = new ObjectInputStream(fileIn);
////					results = (ArrayList<Tuple>)in.readObject();
////					System.out.println(results.toString());
////
////				} catch (IOException e) {
////					e.printStackTrace();
////				} catch (ClassNotFoundException e) {
////					e.printStackTrace();
////				}
////			}
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(15000));
			htblColNameValue.put("name", new String("Seif"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(570));
			htblColNameValue.put("name", new String("Monica"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(8763));
			htblColNameValue.put("name", new String("Abdelrahman"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(15908));
			htblColNameValue.put("name", new String("Shadi"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(3092));
			htblColNameValue.put("name", new String("Emad"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.insertIntoTable(strTableName, htblColNameValue);
			
			ourApp.createBRINIndex(strTableName,"name");
			
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(570));
			htblColNameValue.put("name", new String("Monica Anis"));
			htblColNameValue.put("gpa", new Double(0.95));
			ourApp.updateTable(strTableName, "id", htblColNameValue);
			
			ourApp.createBRINIndex(strTableName,"id");
			
			Object[] objarrValues = {"780","3600"}; 
			String[] strarrOperators = {">","<="};
			//ourApp.selectFromTable(strTableName, "id", objarrValues, strarrOperators);
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(780));
//			htblColNameValue.put("name", new String("Maggie"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			ourApp.deleteFromTable(strTableName, htblColNameValue);
////			
//
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(780));
//			htblColNameValue.put("name ", new String("Maggie"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			ourApp.insertIntoTable(strTableName, htblColNameValue);
////			
//			htblColNameValue.clear();
//			htblColNameValue.put("id", new Integer(570));
//			htblColNameValue.put("name", new String("Monica"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			ourApp.insertIntoTable(strTableName, htblColNameValue);
	
			
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		
		for (int i = 1; i <=5; i++)
		{
			ObjectInputStream in;
			Brin results;
			ArrayList<Tuple> result;
			System.out.println("page" + i);
			try {
				String pagePath = "data/Student Table/Page " + i + ".ser";
				FileInputStream fileIn = new FileInputStream(pagePath);
				in = new ObjectInputStream(fileIn);
				result = (ArrayList<Tuple>) in.readObject();
				System.out.println(result.toString());
			/*	try{
					while((results = (Brin)in.readObject()) != null ) 
						System.out.println(results.getMaximum());
				}
				catch (EOFException e) {
					
				}*/
				
				

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
//	    ArrayList<Object> results = new ArrayList<Object>();
//
//		try {
//
//			 FileInputStream fileIn = new FileInputStream("src/DB2App/Student Table/Page 1.ser");
//			 ObjectInputStream in = new ObjectInputStream(fileIn);
//			
//			Object tn;
//			while ((tn = (in.readObject())) != null)
//			{
//				results.add(tn);
//				System.out.println(((Tuple) tn).toString());
//
//			}
//			
//			
//			
//			
//			 in.close();
//			 
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
//		catch (ClassNotFoundException e) {
//		e.printStackTrace(); 
//		}
//			
	
		
		//inserting some tuples 
		//I'm using the same hashtable table htblColNameValue so make sure u clear it before inserting each time
		

	}

}
