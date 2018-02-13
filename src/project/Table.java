package project;
import java.util.Hashtable;
import java.util.LinkedList;

public class Table
{

	private String strTableName;
	private String strClusteringKeyColumn;
	private Hashtable<String, String> htblColNameType;
	private LinkedList<Page> pages;

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType)
	{
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;
		this.pages = new LinkedList<Page>();
	}

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

	public void setPages(LinkedList<Page> pages)
	{
		this.pages = pages;
	}

}
