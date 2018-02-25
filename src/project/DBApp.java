package project;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import exceptions.DBAppException;
import exceptions.DBInCorrectEntriesNumber;
import exceptions.DBNameInUse;
import exceptions.DBPrimaryKeyNotUnique;
import exceptions.DBPrimaryKeyNull;
import exceptions.DBRowNotFound;
import exceptions.DBTableNotFound;
import exceptions.DBTypeMismatch;
import exceptions.DBUnsupportedType;
import java.lang.String;
import java.time.LocalDateTime;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.*;

public class DBApp
{
	//the max number of allowed objects (tuples/rows) in a page
	//we set it for an arbitrary small number for the sake of testing
	private int maxObjectsPerPage;
	
	//keeping track of all the tables created in our application
	private LinkedList<Table> tablesInApp = new LinkedList<Table>();

	//our initialization
	public void init()
	{
		File file = new File("config/DBApp.properties");
		try {
			FileReader fr = new FileReader(file);
			Properties props = new Properties();
			props.load(fr);
			this.maxObjectsPerPage = Integer.parseInt(props.getProperty("MaximumRowsCountinPage"));
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		   
		//creating our mother directory (folder) that holds all our tables in it and the metadata file as well
		File AppDirectory = new File("data");
		AppDirectory.mkdir();

		//creating the metadata file
		File metaData = new File("data/metaData.csv");
		try
		{
			metaData.createNewFile();
			//adding the header line to metadata file
			String header = "Table Name, Column Name, Column Type, Key, Indexed";
			//adding an empty row
			header += System.lineSeparator();
			PrintWriter out = new PrintWriter("data/metaData.csv");
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
		File newDir = new File("data/" + dirName);
		newDir.mkdir();
		
		// adding Touch date column
		htblColNameType.put("TouchDate", "java.time.LocalDateTime");


		// create table object
		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		
		//add it to the linked list of the application's tables
		tablesInApp.add(t);
		
		//add a new empty page to it, check the method in line 153
		try
		{
			//creating the actual page in the table's directory
			FileWriter fw = new FileWriter("data/" + strTableName+ " Table/Page 1.ser", true);
			FileOutputStream fileOut = new FileOutputStream(
					"data/" + strTableName+ " Table/Page 1.ser");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			
			out.writeObject(new ArrayList<Tuple>());

			out.close();
			fileOut.close();

		} catch (IOException i)
		{
			System.out.println("Could Not Create a Page!");
		}
		createPage(t);

		// storing the table meta data, check line 161
		addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);

	}
	
	public static Page createPage(Table t)
	{
		//creating a new page, for table "t", and with ID equal to the number of pages for that table + 1
		Page p = new Page(t.getStrTableName(), t.getPages().size() + 1);
		
		t.getPages().add(p);
		
		
		

		return p;
	}
	
	public void updateTable(String strTableName, String strKey, Hashtable<String,Object> htblColNameValue ) throws DBAppException
	{
		int index = checkDataInsertionExceptions(strTableName, htblColNameValue);
		htblColNameValue.put("TouchDate", LocalDateTime.now());
		Enumeration<Object> colValues = htblColNameValue.elements();

		//an empty linked list to store in it the tuple's data
		LinkedList<Object> l = new LinkedList<Object>();
		
		//extracting the column names and values, and adding the values to the linked list
		while (colValues.hasMoreElements())
		{
			//String curName = colNames.nextElement();
			Object curValue = colValues.nextElement();

			l.addFirst(curValue);
		}
		//creating a tuple object with a linked list of the tuple's data
		Tuple t = new Tuple(l);
		
		for (int i = 0; i < tablesInApp.size(); i++)
		{
			Table table = tablesInApp.get(i);
			String tableName = table.getStrTableName();
			
			//if that's the table we're looking for
			if (tableName.equals(strTableName))
			{
				//getting the number of pages to perform binary search
				int numPages = table.getPages().size();
				int pageIndex = getPageNumber(strTableName, t, 1, numPages, index);
				int pos = updateInPage(strTableName, pageIndex, t, index);
				if (pos == -1)
				{
					throw new DBRowNotFound();
				}
				else
				{
					ObjectInputStream in;
					ArrayList<Tuple> results = null;
					try {
						String pagePath = "data/" + strTableName + " Table/Page " + pageIndex + ".ser";
						FileInputStream fileIn = new FileInputStream(pagePath);
						in = new ObjectInputStream(fileIn);
						results = (ArrayList<Tuple>)in.readObject();

					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					results.remove(pos);
					results.add(pos, t);
					File file = new File("data/" + strTableName + " Table/Page " + pageIndex + ".ser");
					file.delete();
					try
					{
						//creating the actual page in the table's directory
						FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + pageIndex + ".ser", true);
						FileOutputStream fileOut = new FileOutputStream(
								"data/" + strTableName + " Table/Page " + pageIndex + ".ser");

						ObjectOutputStream out = new ObjectOutputStream(fileOut);

						//System.out.println(pageContent.toString());
						out.writeObject(results);

						out.close();
						fileOut.close();

					} catch (IOException e)
					{
						System.out.println("Could Not Create a Page!");
					}
				}
			}
		}
	}
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
	{
		int index = checkDataInsertionExceptions(strTableName, htblColNameValue);
		Enumeration<Object> colValues = htblColNameValue.elements();

		//an empty linked list to store in it the tuple's data
		LinkedList<Object> l = new LinkedList<Object>();
		
		//extracting the column names and values, and adding the values to the linked list
		while (colValues.hasMoreElements())
		{
			//String curName = colNames.nextElement();
			Object curValue = colValues.nextElement();

			l.addFirst(curValue);
		}
		//creating a tuple object with a linked list of the tuple's data
		Tuple t = new Tuple(l);
		
		for (int i = 0; i < tablesInApp.size(); i++)
		{
			Table table = tablesInApp.get(i);
			String tableName = table.getStrTableName();
			
			//if that's the table we're looking for
			if (tableName.equals(strTableName))
			{
				//getting the number of pages to perform binary search
				int numPages = table.getPages().size();
				int pageIndex = getPageNumber(strTableName, t, 1, numPages, index);
				int pos = deleteFromPage(strTableName, pageIndex, t, index);
				if (pos == -1)
				{
					throw new DBRowNotFound();
				}
				else
				{
					ObjectInputStream in;
					ArrayList<Tuple> results = null;
					try {
						String pagePath = "data/" + strTableName + " Table/Page " + pageIndex + ".ser";
						FileInputStream fileIn = new FileInputStream(pagePath);
						in = new ObjectInputStream(fileIn);
						results = (ArrayList<Tuple>)in.readObject();

					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}
					results.remove(pos);
					if (results.size() == 0)
					{
						shiftDown(strTableName, pageIndex, numPages);
						table.getPages().remove(table.getPages().size() -1);
					}
					else
					{
						File file = new File("data/" + strTableName + " Table/Page " + pageIndex + ".ser");
						file.delete();
						try
						{
							//creating the actual page in the table's directory
							FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + pageIndex + ".ser", true);
							FileOutputStream fileOut = new FileOutputStream(
									"data/" + strTableName + " Table/Page " + pageIndex + ".ser");

							ObjectOutputStream out = new ObjectOutputStream(fileOut);

							//System.out.println(pageContent.toString());
							out.writeObject(results);

							out.close();
							fileOut.close();

						} catch (IOException e)
						{
							System.out.println("Could Not Create a Page!");
						}
					}
				}
				
			}
		}

	}
	
	private void shiftDown(String strTableName, int start, int end) {
		if (start == end)
		{

			try
			{
				//creating the actual page in the table's directory
				FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + end + ".ser", true);
				FileOutputStream fileOut = new FileOutputStream(
						"data/" + strTableName + " Table/Page " + end + ".ser");

				ObjectOutputStream out = new ObjectOutputStream(fileOut);

				//System.out.println(pageContent.toString());
				out.writeObject(null);

				out.close();
				fileOut.close();

			} catch (IOException e)
			{
				System.out.println("Could Not Create a Page!");
			}

		}
		
		for (int i = start+1; i <= end; i++)
		{
			ObjectInputStream in;
			ArrayList<Tuple> results = null;
			try {
				String pagePath = "data/" + strTableName + " Table/Page " + i + ".ser";
				FileInputStream fileIn = new FileInputStream(pagePath);
				in = new ObjectInputStream(fileIn);
				results = (ArrayList<Tuple>)in.readObject();
				
				FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " +(i-1)+ ".ser", true);
				FileOutputStream fileOut = new FileOutputStream(
						"data/" + strTableName + " Table/Page " + (i-1) + ".ser");

				ObjectOutputStream out = new ObjectOutputStream(fileOut);

				
				out.writeObject(results);

				out.close();
				fileOut.close();

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
			if (end == i)
			{
				try
				{
					//creating the actual page in the table's directory
					FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + end + ".ser", true);
					FileOutputStream fileOut = new FileOutputStream(
							"data/" + strTableName + " Table/Page " + end + ".ser");

					ObjectOutputStream out = new ObjectOutputStream(fileOut);

					//System.out.println(pageContent.toString());
					out.writeObject(null);

					out.close();
					fileOut.close();

				} catch (IOException e)
				{
					System.out.println("Could Not Create a Page!");
				}

			}
			
		}
	}

	private int updateInPage(String strTableName, int pageIndex, Tuple t, int clusteringKeyIndex) {
		ObjectInputStream in;
		ArrayList<Tuple> results = null;
		try {
			String pagePath = "data/" + strTableName + " Table/Page " + pageIndex + ".ser";
			FileInputStream fileIn = new FileInputStream(pagePath);
			in = new ObjectInputStream(fileIn);
			results = (ArrayList<Tuple>)in.readObject();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//System.out.println("fetched results: " + results.toString());
		int position = 0;
		//System.out.println(t.getTupleInfo().get(clusteringKeyIndex).getClass().toString());
		for (Tuple tuple: results)
		{
			if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.String"))
			{
				if ( ((String)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((String)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
				{
					return position;
				}
				
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Integer"))
			{
				//System.out.println("test");
				if ( ((Integer)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Integer)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
				{
					return position;
				}
				
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Double"))
			{
				if ( ((Double)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Double)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
				{
					return position;
				}
				
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.util.Date"))
			{
				if ( ((Date)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Date)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
				{
					return position;
				}
			
			}
			position++;
		}
		return -1;
		
	}
	
	private int deleteFromPage(String strTableName, int pageIndex, Tuple t, int clusteringKeyIndex) {
		ObjectInputStream in;
		ArrayList<Tuple> results = null;
		try {
			String pagePath = "data/" + strTableName + " Table/Page " + pageIndex + ".ser";
			FileInputStream fileIn = new FileInputStream(pagePath);
			in = new ObjectInputStream(fileIn);
			results = (ArrayList<Tuple>)in.readObject();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//System.out.println("fetched results: " + results.toString());
		int position = 0;
		//System.out.println(t.getTupleInfo().get(clusteringKeyIndex).getClass().toString());
		for (Tuple tuple: results)
		{
			for (int i = 0; i < t.getTupleInfo().size(); i++)
			{
				if ((t.getTupleInfo().get(i).getClass().toString().substring(6)).equals("java.lang.String"))
				{
					if ( ((String)(t.getTupleInfo().get(i))) .compareTo  
							((String)(tuple.getTupleInfo().get(i))) != 0)
					{
						break;
					}
					
				}
				else if ((t.getTupleInfo().get(i).getClass().toString().substring(6)).equals("java.lang.Integer"))
				{
					//System.out.println("test");
					if ( ((Integer)(t.getTupleInfo().get(i))) .compareTo  
							((Integer)(tuple.getTupleInfo().get(i))) != 0)
					{
						break;
					}
					
				}
				else if ((t.getTupleInfo().get(i).getClass().toString().substring(6)).equals("java.lang.Double"))
				{
					if ( ((Double)(t.getTupleInfo().get(i))) .compareTo  
							((Double)(tuple.getTupleInfo().get(i))) != 0)
					{
						break;
					}
					
				}
				else if ((t.getTupleInfo().get(i).getClass().toString().substring(6)).equals("java.util.Date"))
				{
					if ( ((Date)(t.getTupleInfo().get(i))) .compareTo  
							((Date)(tuple.getTupleInfo().get(i))) != 0)
					{
						break;
					}
				
				}
				if (i == t.getTupleInfo().size() - 1)
					return position;
			}

			position++;
		}
		return -1;
		
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		//Check for exceptions
		int index = checkDataInsertionExceptions(strTableName, htblColNameValue);
				
		//Adding DateTime at the time of inserting the tuple
		htblColNameValue.put("TouchDate", LocalDateTime.now());
				
		//******** initialize the tuple with the information:

		/*check http://www.java2s.com/Tutorial/Java/0140__Collections/FetchingKeysandValuesthegetmethod.htm
		  for the use of enumeration */
			
		//getting the column values
		Enumeration<Object> colValues = htblColNameValue.elements();

		//an empty linked list to store in it the tuple's data
		LinkedList<Object> l = new LinkedList<Object>();
		
		//extracting the column names and values, and adding the values to the linked list
		while (colValues.hasMoreElements())
		{
			//String curName = colNames.nextElement();
			Object curValue = colValues.nextElement();

			l.addFirst(curValue);
		}
		//creating a tuple object with a linked list of the tuple's data
		Tuple t = new Tuple(l);
		
		//System.out.println(index);
		//System.out.println(l.toString());
		
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
				//getting the number of pages to perform binary search
				pagesOfTable = table.getPages();
				int numPages = pagesOfTable.size();
				int pageIndex = getPageNumber(strTableName, t, 1, numPages, index);
				//System.out.print("tuple will be inserted in " + pageIndex);
				
				ArrayList<Tuple> pageContent = insertIntoPage(strTableName, pageIndex, t, index);
				if (pageContent.size() > maxObjectsPerPage)
				{
					
					//renaming all following files
					shift(strTableName, pageIndex, numPages);
//					for (int k = numPages; k >  pageIndex; k-=1)
//					{
//						System.out.println("k: " + k);
//						File oldfile = new File("data/" + strTableName + " Table/Page " + k + ".ser");
//						File newfile = new File("data/" + strTableName + " Table/Page " + (k+1) + ".ser");
//						if (newfile.exists())
//							System.out.println(newfile.getPath());
//						if(oldfile.renameTo(newfile))
//							System.out.println("success");
//						else
//							System.out.println("fail");
//					}
					Page p = new Page(tableName, numPages + 1);
					table.getPages().add(p);
					
					//dividing content to two pages
					ArrayList<Tuple> newPageContent = new ArrayList<Tuple>();
					for (int j = maxObjectsPerPage/2; j <= maxObjectsPerPage; j++)
						newPageContent.add(pageContent.remove(maxObjectsPerPage/2));
					File file = new File("data/" + strTableName + " Table/Page " + pageIndex + ".ser");
					file.delete();
					try
					{
						//creating the actual page in the table's directory
						FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + pageIndex + ".ser", true);
						FileOutputStream fileOut = new FileOutputStream(
								"data/" + strTableName + " Table/Page " + pageIndex + ".ser");

						ObjectOutputStream out = new ObjectOutputStream(fileOut);

						//System.out.println(pageContent.toString());
						//System.out.println(newPageContent.toString());
						out.writeObject(pageContent);

						
						
						fw = new FileWriter("data/" + strTableName + " Table/Page " + (pageIndex+1) + ".ser", true);
						fileOut = new FileOutputStream(
								"data/" + strTableName + " Table/Page " + (pageIndex+1) + ".ser");

						out = new ObjectOutputStream(fileOut);

						
						out.writeObject(newPageContent);

						out.close();
						fileOut.close();

					} catch (IOException e)
					{
						System.out.println("Could Not Create a Page!");
					}
					
				} 
				else
				{
					File file = new File("data/" + strTableName + " Table/Page " + pageIndex + ".ser");
					file.delete();
					try
					{
						//creating the actual page in the table's directory
						FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " + pageIndex + ".ser", true);
						FileOutputStream fileOut = new FileOutputStream(
								"data/" + strTableName + " Table/Page " + pageIndex + ".ser");

						ObjectOutputStream out = new ObjectOutputStream(fileOut);

						//System.out.println(pageContent.toString());
						out.writeObject(pageContent);

						out.close();
						fileOut.close();

					} catch (IOException e)
					{
						System.out.println("Could Not Create a Page!");
					}
				}
			}
		}		 
				

	}

	private void shift(String strTableName, int start, int end) {
		for (int i = end; i > start; i--)
		{
			ObjectInputStream in;
			ArrayList<Tuple> results = null;
			try {
				String pagePath = "data/" + strTableName + " Table/Page " + i + ".ser";
				FileInputStream fileIn = new FileInputStream(pagePath);
				in = new ObjectInputStream(fileIn);
				results = (ArrayList<Tuple>)in.readObject();
				
				FileWriter fw = new FileWriter("data/" + strTableName + " Table/Page " +(i+1)+ ".ser", true);
				FileOutputStream fileOut = new FileOutputStream(
						"data/" + strTableName + " Table/Page " + (i+1) + ".ser");

				ObjectOutputStream out = new ObjectOutputStream(fileOut);

				
				out.writeObject(results);

				out.close();
				fileOut.close();

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
			
			
		}
	}
	


	private ArrayList<Tuple> insertIntoPage(String strTableName, int pageIndex, Tuple t, int clusteringKeyIndex) throws DBPrimaryKeyNotUnique {
		ObjectInputStream in;
		ArrayList<Tuple> results = null;
		try {
			String pagePath = "data/" + strTableName + " Table/Page " + pageIndex + ".ser";
			FileInputStream fileIn = new FileInputStream(pagePath);
			in = new ObjectInputStream(fileIn);
			results = (ArrayList<Tuple>)in.readObject();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		//System.out.println("fetched results: " + results.toString());
		int position = 0;
		//System.out.println(t.getTupleInfo().get(clusteringKeyIndex).getClass().toString());
		for (Tuple tuple: results)
		{
			if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.String"))
			{
				if ( ((String)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((String)(tuple.getTupleInfo().get(clusteringKeyIndex))) < 0)
				{
					results.add(position, t);
					return results;
				}
				else if ( ((String)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((String)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
					throw new DBPrimaryKeyNotUnique();
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Integer"))
			{
				//System.out.println("test");
				if ( ((Integer)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Integer)(tuple.getTupleInfo().get(clusteringKeyIndex))) < 0)
				{
					results.add(position, t);
					return results;
				}
				else if ( ((Integer)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Integer)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
					throw new DBPrimaryKeyNotUnique();
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Double"))
			{
				if ( ((Double)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Double)(tuple.getTupleInfo().get(clusteringKeyIndex))) < 0)
				{
					results.add(position, t);
					return results;
				}
				else if ( ((Double)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Double)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
					throw new DBPrimaryKeyNotUnique();
			}
			else if ((t.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.util.Date"))
			{
				if ( ((Date)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Date)(tuple.getTupleInfo().get(clusteringKeyIndex))) < 0)
				{
					results.add(position, t);
					return results;
				}
				else if ( ((Date)(t.getTupleInfo().get(clusteringKeyIndex))) .compareTo  
						((Date)(tuple.getTupleInfo().get(clusteringKeyIndex))) == 0)
					throw new DBPrimaryKeyNotUnique();
			}
			position++;
		}
		results.add(position, t);
		//System.out.println("Results: " + results.toString());
		return results;
		
	}

	private int getPageNumber(String strTableName, Tuple data, int start, int end, int clusteringKeyIndex)
	{
		//System.out.println("start: " + start + "end: " + end);
		int index = (start + end)/2;
		
		if (start == end)
	    	return index;
		if (index == 0)
			return 1;
		ObjectInputStream in;
		ArrayList<Tuple> results = null;
		try {
			String pagePath = "data/" + strTableName + " Table/Page " + index + ".ser";
			//System.out.println(index + "fady");
			FileInputStream fileIn = new FileInputStream(pagePath);
			in = new ObjectInputStream(fileIn);
			results = (ArrayList<Tuple>)in.readObject();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		
		if ((data.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.String"))
		{
			if ( ((String)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (String)(results.get(0).getTupleInfo().get(clusteringKeyIndex))) < 0 )
				return getPageNumber(strTableName, data, start, index - 1, clusteringKeyIndex);
			else if ( ((String)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (String)(results.get(results.size() - 1).getTupleInfo().get(clusteringKeyIndex))) > 0 )
				return getPageNumber(strTableName, data, index + 1, end, clusteringKeyIndex);
			else 
				return index;
		}
		else if ((data.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Integer"))
		{
			if ( ((Integer)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Integer)(results.get(0).getTupleInfo().get(clusteringKeyIndex))) < 0 )
				return getPageNumber(strTableName, data, start, index - 1, clusteringKeyIndex);
			else if ( ((Integer)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Integer)(results.get(results.size()-1).getTupleInfo().get(clusteringKeyIndex))) > 0 )
				return getPageNumber(strTableName, data, index + 1, end, clusteringKeyIndex);
			else 
				return index;
		}
		else if ((data.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.lang.Double"))
		{
			if ( ((Double)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Double)(results.get(0).getTupleInfo().get(clusteringKeyIndex))) < 0 )
				return getPageNumber(strTableName, data, start, index - 1, clusteringKeyIndex);
			else if ( ((Double)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Double)(results.get(results.size()-1).getTupleInfo().get(clusteringKeyIndex))) > 0 )
				return getPageNumber(strTableName, data, index + 1, end, clusteringKeyIndex);
			else 
				return index;
		}
		else if ((data.getTupleInfo().get(clusteringKeyIndex).getClass().toString().substring(6)).equals("java.util.Date"))
		{
			if ( ((Date)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Date)(results.get(0).getTupleInfo().get(clusteringKeyIndex))) < 0 )
				return getPageNumber(strTableName, data, start, index - 1, clusteringKeyIndex);
			else if ( ((Date)(data.getTupleInfo().get(clusteringKeyIndex))).compareTo 
					( (Date)(results.get(results.size()-1).getTupleInfo().get(clusteringKeyIndex))) > 0 )
				return getPageNumber(strTableName, data, index + 1, end, clusteringKeyIndex);
			else 
				return index;
		}
		
	    
	    
	    
	


//	    data.getTupleInfo().get(clusteringKeyIndex).getClass().toString();
//		data.getTupleInfo().get(clusteringKeyIndex);
//		results.get(0).getTupleInfo().get(results.size()-1);
	    return 0;

	}
	
	

	//htblColNameType is a hashtable with key: column name (String), and value: column value (Object)
	//eg. for <Key,Value> : <"id",375>
//	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
//	{
//		
//		
//		//Check for exceptions
//		int index = checkDataInsertionExceptions(strTableName, htblColNameValue);
//		
//		//Adding DateTime at the time of inserting the tuple
//		htblColNameValue.put("TouchDate", LocalDateTime.now());
//		
//		//******** initialize the tuple with the information:
//
//		/*check http://www.java2s.com/Tutorial/Java/0140__Collections/FetchingKeysandValuesthegetmethod.htm
//		  for the use of enumeration */
//		
//		//getting the column names
//		Enumeration<String> colNames = htblColNameValue.keys();
//		//getting the column values
//		Enumeration<Object> colValues = htblColNameValue.elements();
//
//		//an empty linked list to store in it the tuple's data
//		LinkedList<Object> l = new LinkedList<Object>();
//
//		//extracting the column names and values, and adding the values to the linked list
//		while (colValues.hasMoreElements())
//		{
//			//String curName = colNames.nextElement();
//			Object curValue = colValues.nextElement();
//
//			l.addFirst(curValue);
//		}
//		//creating a tuple object with a linked list of the tuple's data
//		Tuple t = new Tuple(l);
//
//		//********* writing the tuple to a page::
//		//we need to locate the right page that we should write the tuple to
//		//that is (for now), the last p age in the table that has space
//		//if it doesn't have space, we shift
//		//*********** NOW THAT NEEDS TO CHANGE, WE NEED TO FIND THE RIGHT PAGE SO THAT THE DATA IS SORTED =D
//		
//		//a linked list of the pages that hold the table's data
//		LinkedList<Page> pagesOfTable = null;
//		//the last page in the pages that holds the table's data
//		Page lastPage = null;
//
//		//looping on all our app's tables to find the table with the table name passed to the method
//		//ie the table that the user wants to insert in
//		for (int i = 0; i < tablesInApp.size(); i++)
//		{
//			Table table = tablesInApp.get(i);
//			String tableName = table.getStrTableName();
//			
//			//if that's the table we're looking for
//			if (tableName.equals(strTableName))
//			{
//				//getting the last page of that table
//				pagesOfTable = table.getPages();
//				lastPage = pagesOfTable.getLast();
//
//				//if the last page is full, create a new empty page
//				if (lastPage.getNumOfTuples() == maxObjectsPerPage)
//				{
//
//					Page p = createPage(table);
//					//make the 'former' last page refers to the 'new' last page "p"
//					pagesOfTable.getLast().setNextPage(p);
//					//adding the newly created page to the table's linked list of pages
//					(tablesInApp.get(i).getPages()).add(p);
//					//getting the newly created last page, that's the page we'll insert in
//					lastPage = tablesInApp.get(i).getPages().getLast();
//				}
//				//incementing the number of tuples in that page as we'll insert in it a new tuple
//				lastPage.setNumOfTuples(pagesOfTable.getLast().getNumOfTuples() + 1);
//				;
//				break;
//			}
//		}
//
//		try
//		{
//			//getting the id of the last page, ie the page we'll insert in 
//			int pageID = lastPage.getID();
//
//			//locating the page in the table's folder
//			FileOutputStream fileOut = new FileOutputStream(
//					"data/" + strTableName + " Table/Page " + pageID + ".ser");
//
//			ObjectOutputStream out = new ObjectOutputStream(fileOut);
//
//			//.writeObject(Object) is a method for serializable
//			// check https://www.tutorialspoint.com/java/java_serialization.htm to see how serialization and deserialization works =D
//			out.writeObject(t);
//
//			out.close();
//			fileOut.close();
//
//			//uncomment that to see the tuple's data deserialized
//			//don't forget to uncomment the catch part as well, line 193
//			
//			 FileInputStream fileIn = new FileInputStream("data/" +
//			 strTableName + " Table/Page " + pageID + ".ser");
//			 ObjectInputStream in = new ObjectInputStream(fileIn); Tuple tn = (Tuple)(in.readObject()); 
//			 System.out.println(tn.toString());
//			 in.close();
//
//		} catch (IOException i)
//		{
//			i.printStackTrace();
//		} 
//		catch (ClassNotFoundException e) {
//			e.printStackTrace(); 
//		}
//			 
//
//	}



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
			FileWriter fw = new FileWriter("data/metaData.csv", true);
			BufferedWriter bw = new BufferedWriter(fw);

			PrintWriter out = new PrintWriter(bw);
			out.println(meta);
			out.close();

		} catch (IOException i)
		{

			System.out.println("Metadata File Not Found!");
		}
	}
	
	public int checkDataInsertionExceptions(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException
	{
		/*
		 * Incorrect Entries number DONE
		 * primary key not unique
		 * primary key null Done
		 * type mismatch Done
		 */
		int keyIndex = 0;
		int args = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/metaData.csv"));
			String line = br.readLine();
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
					{
						key = content[1];
						keyIndex = args;
					}
				}
				line = br.readLine();
			}
			br.close();
			//System.out.println(meta.toString());
			//System.out.println(htblColNameValue.toString());
			if (args == 0)
				throw new DBTableNotFound(strTableName);
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
		//System.out.println(args - keyIndex);
		return args - keyIndex;
		
	}
	
	public void checkTableCreationException(String strTableName, Hashtable<String, String> htblColNameType) throws DBAppException 
	{

		//checking that the metadata file doesnnot contain anothertable with the same name
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/metaData.csv"));
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
