package project;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;

import com.sun.xml.internal.bind.v2.schemagen.xmlschema.List;

import exceptions.DBAppException;
import exceptions.DBInCorrectEntriesNumber;
import exceptions.DBNameInUse;
import exceptions.DBPrimaryKeyNull;
import exceptions.DBTypeMismatch;
import exceptions.DBUnsupportedType;

import java.lang.Integer;
import java.lang.String;
import java.time.LocalDateTime;
import java.lang.Double;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

public class DBApp
{
	//the max number of allowed objects (tuples/rows) in a page
	//we set it for an arbitrary small number for the sake of testing
	private final int maxObjectsPerPage = 4;
	
	//keeping track of all the tables created in our application
	private LinkedList<Table> tablesInApp = new LinkedList<Table>();

	//our initialization
	public void init()
	{
		//creating our mother directory (folder) that holds all our tables in it and the metadata file as well
		File AppDirectory = new File("src/DB2App");
		AppDirectory.mkdir();

		//creating the metadata file
		File metaData = new File("src/DB2App/metaData.csv");
		try
		{
			metaData.createNewFile();
			//adding the header line to metadata file
			String header = "Table Name, Column Name, Column Type, Key, Indexed";
			//adding an empty row
			header += System.lineSeparator();
			PrintWriter out = new PrintWriter("src/DB2App/metaData.csv");
			out.println(header);
			out.close();
		} catch (IOException e)
		{
			System.out.println("Metadata File Not Found!");
		}
	}

	//create table method, its name says it all =P
	//strClusteringKeyColumn = primary key
	//htblColNameType is a hashtable with key: column name (String), and value: column type (String)
	//eg. for <Key,Value> : <"id","java.lang.Integer">
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException
	{
		//check for table creation exceptions
		checkTableCreationException(strTableName, htblColNameType);

		// making a directory for the table
		String dirName = strTableName + " Table";
		File newDir = new File("src/DB2App/" + dirName);
		newDir.mkdir();
		
		// adding Touch date column
		htblColNameType.put("TouchDate", "java.time.LocalDateTime");


		// create table object
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		
		//add it to the linked list of the application's tables
		tablesInApp.add(t);
		
		//add a new empty page to it, check the method in line 153
		createPage(t);

		// storing the table meta data, check line 161
		addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);

	}

	//htblColNameType is a hashtable with key: column name (String), and value: column value (Object)
	//eg. for <Key,Value> : <"id",375>
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		
		
		//Check for exceptions
		checkDataInsertionExceptions(strTableName, htblColNameValue);
		
		//Adding DateTime at the time of inserting the tuple
		htblColNameValue.put("TouchDate", LocalDateTime.now());
		
		//******** initialize the tuple with the information:

		/*check http://www.java2s.com/Tutorial/Java/0140__Collections/FetchingKeysandValuesthegetmethod.htm
		  for the use of enumeration */
		
		//getting the column names
		Enumeration<String> colNames = htblColNameValue.keys();
		//getting the column values
		Enumeration<Object> colValues = htblColNameValue.elements();

		//an empty linked list to store in it the tuple's data
		LinkedList<Object> l = new LinkedList<Object>();

		//extracting the column names and values, and adding the values to the linked list
		while (colNames.hasMoreElements())
		{
			String curName = colNames.nextElement();
			Object curValue = colValues.nextElement();

			l.addFirst(curValue);
		}
		
		//creating a tuple object with a linked list of the tuple's data
		Tuple t = new Tuple(l);

		//********* writing the tuple to a page::
		//we need to locate the right page that we should write the tuple to
		//that is (for now), the last p age in the table that has space
		//if it doesn't have space, we shift
		//*********** NOW THAT NEEDS TO CHANGE, WE NEED TO FIND THE RIGHT PAGE SO THAT THE DATA IS SORTED =D
		
		//a linked list of the pages that hold the table's data
		LinkedList<Page> pagesOfTable = null;
		//the last page in the pages that holds the table's data
		Page lastPage = null;

		//looping on all our app's tables to find the table with the table name passed to the method
		//ie the table that the user wants to insert in
		for (int i = 0; i < tablesInApp.size(); i++)
		{
			Table table = tablesInApp.get(i);
			String tableName = table.getStrTableName();
			
			//if that's the table we're looking for
			if (tableName.equals(strTableName))
			{
				//getting the last page of that table
				pagesOfTable = table.getPages();
				lastPage = pagesOfTable.getLast();

				//if the last page is full, create a new empty page
				if (lastPage.getNumOfTuples() == maxObjectsPerPage)
				{

					Page p = createPage(table);
					//make the 'former' last page refers to the 'new' last page "p"
					pagesOfTable.getLast().setNextPage(p);
					//adding the newly created page to the table's linked list of pages
					(tablesInApp.get(i).getPages()).add(p);
					//getting the newly created last page, that's the page we'll insert in
					lastPage = tablesInApp.get(i).getPages().getLast();
				}
				//incementing the number of tuples in that page as we'll insert in it a new tuple
				lastPage.setNumOfTuples(pagesOfTable.getLast().getNumOfTuples() + 1);
				;
				break;
			}
		}

		try
		{
			//getting the id of the last page, ie the page we'll insert in 
			int pageID = lastPage.getID();

			//locating the page in the table's folder
			FileOutputStream fileOut = new FileOutputStream(
					"src/DB2App/" + strTableName + " Table/Page " + pageID + ".ser");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			//.writeObject(Object) is a method for serializable
			// check https://www.tutorialspoint.com/java/java_serialization.htm to see how serialization and deserialization works =D
			out.writeObject(t);

			out.close();
			fileOut.close();

			//uncomment that to see the tuple's data deserialized
			//don't forget to uncomment the catch part as well, line 193
			
			 FileInputStream fileIn = new FileInputStream("src/DB2App/" +
			 strTableName + " Table/Page " + pageID + ".ser");
			 ObjectInputStream in = new ObjectInputStream(fileIn); Tuple tn = (Tuple)(in.readObject()); 
			 System.out.println(tn.toString());
			 

		} catch (IOException i)
		{
			i.printStackTrace();
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace(); 
		}
			 

	}

	public static Page createPage(Table t)
	{
		//creating a new page, for table "t", and with ID equal to the number of pages for that table + 1
		Page p = new Page(t.getStrTableName(), t.getPages().size() + 1);
		//adding this newly created page to the table's list of pages
		t.getPages().add(p);

		return p;
	}

	public static void addMetaData(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)
	{
		//the string that will hold the columns' data
		String meta = "";

		//getting the column names and types from the hashtable
		Enumeration<String> names = htblColNameType.keys();
		Enumeration<String> types = htblColNameType.elements();

		while (names.hasMoreElements())
		{
			String curName = names.nextElement();
			String curType = types.nextElement();
			
			//adding the column's data
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
			//writing the data to the csv file
			FileWriter fw = new FileWriter("src/DB2App/metaData.csv", true);
			BufferedWriter bw = new BufferedWriter(fw);

			PrintWriter out = new PrintWriter(bw);
			out.println(meta);
			out.close();

		} catch (IOException i)
		{

			System.out.println("Metadata File Not Found!");
		}
	}
	
	public void checkDataInsertionExceptions(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		/*
		 * Incorrect Entries number DONE
		 * primary key not unique
		 * primary key null Done
		 * type mismatch Done
		 */
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/DB2App/metaData.csv"));
			String line = br.readLine();
			int args = 0;
			String key = null;
			Hashtable<String, String> meta = new Hashtable<String, String>();

			while (line != null)
			{
				String[] content = line.split(", ");
				if (strTableName.equals(content[0]))
				{
					if (!(content[1].equals("TouchDate")))
					{
						args++;
						meta.put(content[1], content[2]);
					}
					if (content[3].equals("True"))
						key = content[1];
				}
				line = br.readLine();
			}
			System.out.println(meta.toString());
			System.out.println(htblColNameValue.toString());
			if (htblColNameValue.size() != args)
			{
				throw new DBInCorrectEntriesNumber(args);
			}
			Enumeration<String> types = meta.elements();
			Enumeration<String> colNames = htblColNameValue.keys();
			Iterator<Object> colValues = htblColNameValue.values().iterator();			
			while (types.hasMoreElements())
			{
				String type = types.nextElement();
				String name = colNames.nextElement();
				Object value = colValues.next();
				
				if (name.equals(key)
						&& value == null)
					throw new DBPrimaryKeyNull(name);

				if (!(value.getClass().toString().substring(6)).equals(type))
				{
					throw new DBTypeMismatch(type, name);
				}


			}



			
		} catch (IOException e) {
			System.out.println("Metadata File Not Found!");
		}
		
		
		
	}
	
	public void checkTableCreationException(String strTableName, Hashtable<String, String> htblColNameType) throws DBAppException 
	{

		//checking that the metadata file doesnnot contain anothertable with the same name
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/DB2App/metaData.csv"));
			String line = br.readLine();
			while (line != null)
			{
				String[] content = line.split(", ");
				if (content[0] != null
						&& strTableName.equals(content[0]))
				{
					throw new DBNameInUse(strTableName);
				}
				line = br.readLine();
			}
			br.close();
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//checking that all the column types are supported by our app
		String[] valid = {"java.lang.Integer", "java.lang.String", "java.lang.Double", "java.lang.Boolean", "java.util.Date"};
		Enumeration<String> types = htblColNameType.elements();
		while (types.hasMoreElements())
		{
			String type = types.nextElement();
			for (int i = 0; i < valid.length; i++)
			{
				if (type.equals(valid[i]))
					break;
				if (i == 4)
					throw new DBUnsupportedType(type);
			}

			
			
		}

	}
	
	

}
