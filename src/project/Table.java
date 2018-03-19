package project;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;

public class Table
{

	//table name
	private String strTableName;
	//primary key
	private String strClusteringKeyColumn;
	//hashtable with key: column name, and value: column type
	private Hashtable<String, String> htblColNameType;
	//a linked list of the pages that hold the table's data
	private LinkedList<Page> pages;
	//
	private ArrayList<String> indexedColumns;

	//constructor
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType)
	{
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		//when the table is created it has no pages yet
		this.pages = new LinkedList<Page>();
		//when the table is created it has no indices yet
		this.indexedColumns = new ArrayList<String>();
	}

	//getters and setters
	public String getStrTableName()
	{
		return strTableName;
	}

	public void setStrTableName(String strTableName)
	{
		this.strTableName = strTableName;
	}

	public String getStrClusteringKeyColumn()
	{
		return strClusteringKeyColumn;
	}

	public void setStrClusteringKeyColumn(String strClusteringKeyColumn)
	{
		this.strClusteringKeyColumn = strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType()
	{
		return htblColNameType;
	}

	public void setHtblColNameType(Hashtable<String, String> htblColNameType)
	{
		this.htblColNameType = htblColNameType;
	}

	public LinkedList<Page> getPages()
	{
		return pages;
	}
	
	public ArrayList<String> getIndexedColumns()
	{
		return indexedColumns;
	}
	
	public void setPages(LinkedList<Page> pages)
	{
		this.pages = pages;
	}

}
