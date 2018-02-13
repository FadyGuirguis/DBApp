package project;

import java.util.Hashtable;
import java.util.LinkedList;
import java.lang.Integer;
import java.lang.String;
import java.lang.Double;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Enumeration;

public class DBApp
{
	private final int maxObjectsPerPage = 4;
	private LinkedList<Table> tablesInApp = new LinkedList<Table>();

	public static void main(String[] args)
	{
		DBApp ourApp = new DBApp();
		ourApp.init();

		String strTableName = "Student";
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

	public void init()
	{

		File AppDirectory = new File("src/DB2App");
		AppDirectory.mkdir();

		File metaData = new File("src/DB2App/metaData.txt");
		try
		{
			metaData.createNewFile();
			String header = "Table Name, Column Name, Column Type, Key, Indexed";
			header += System.lineSeparator();
			PrintWriter out = new PrintWriter("src/DB2App/metaData.txt");
			out.println(header);
			out.close();
		} catch (IOException e)
		{
			System.out.println("File Not Found!");
		}
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)
	{

		// making a directory for the table
		String dirName = strTableName + " Table";
		File newDir = new File("src/DB2App/" + dirName);
		newDir.mkdir();

		// create table object
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		tablesInApp.add(t);
		createPage(t);

		// storing the table meta data
		addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
	{
		// initialize the tuple with the information

		Enumeration<String> names = htblColNameValue.keys();
		Enumeration<Object> values = htblColNameValue.elements();

		LinkedList<Object> l = new LinkedList<Object>();

		while (names.hasMoreElements())
		{
			String curName = names.nextElement();
			Object curValue = values.nextElement();

			l.add(curValue);
		}

		Tuple t = new Tuple(l);

		// writing the tuple to a page

		LinkedList<Page> pagesOfTable = null;
		Page lastPage = null;

		for (int i = 0; i < tablesInApp.size(); i++)
		{
			if ((tablesInApp.get(i).getStrTableName()).equals(strTableName))
			{
				pagesOfTable = tablesInApp.get(i).getPages();
				lastPage = pagesOfTable.getLast();

				if (lastPage.getNumOfTuples() == maxObjectsPerPage)
				{

					Page p = createPage(tablesInApp.get(i));
					pagesOfTable.getLast().setNextPage(p);
					(tablesInApp.get(i).getPages()).add(p);
					lastPage = tablesInApp.get(i).getPages().getLast();
				}
				pagesOfTable.getLast().setNumOfTuples(pagesOfTable.getLast().getNumOfTuples() + 1);
				;
				break;
			}
		}

		try
		{

			int pageID = lastPage.getID();

			FileOutputStream fileOut = new FileOutputStream(
					"src/DB2App/" + strTableName + " Table/Page " + pageID + ".ser");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(t);

			out.close();
			fileOut.close();

			/*
			 * FileInputStream fileIn = new FileInputStream("src/DB2App/" +
			 * strTableName + " Table/Page " + pageID + ".ser");
			 * ObjectInputStream in = new ObjectInputStream(fileIn); Tuple tn =
			 * (Tuple)(in.readObject()); System.out.println(tn.toString());
			 */

		} catch (IOException i)
		{
			i.printStackTrace();
		} /*
			 * catch (ClassNotFoundException e) { // TODO Auto-generated catch
			 * block e.printStackTrace(); }
			 */

	}

	public static Page createPage(Table t)
	{
		Page p = new Page(t.getStrTableName(), t.getPages().size() + 1);
		t.getPages().add(p);

		return p;
	}

	public static void addMetaData(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)
	{
		String meta = "";

		Enumeration<String> names = htblColNameType.keys();
		Enumeration<String> types = htblColNameType.elements();

		while (names.hasMoreElements())
		{
			String curName = names.nextElement();
			String curType = types.nextElement();
			meta += strTableName + ", ";
			meta += curName + ", ";
			meta += curType + ", ";
			if (curName.equals(strClusteringKeyColumn))
			{
				meta += "True, ";
			} else
			{
				meta += "False, ";
			}
			meta += "False ";
			meta += System.lineSeparator();

		}
		try
		{
			FileWriter fw = new FileWriter("src/DB2App/metaData.csv", true);
			BufferedWriter bw = new BufferedWriter(fw);

			PrintWriter out = new PrintWriter(bw);
			out.println(meta);
			out.close();

		} catch (IOException i)
		{

			System.out.println("File Not Found!");
		}
	}

}
