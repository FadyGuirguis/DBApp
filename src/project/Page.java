package project;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class Page
{
	//id of the page: each page in a given table is given a unique id used for naming the files in the table folder
	private int ID = 0;
	//number of tuples inserted in the page, used to know whether the page is full or not yet
	private int numOfTuples;
	//each page has a reference to the next page after it
	private Page nextPage;
	//the name of the table whose data are stored in this page, used to locate the table's folder to make a new page in it
	private String ownerTable;

	public Page(String ownerTable, int ID)
	{
		this.ID = ID;
		this.numOfTuples = 0;
		this.nextPage = null;
		this.ownerTable = ownerTable;

		
	}

	//getters and setters
	public int getID()
	{
		return ID;
	}

	public void setID(int iD)
	{
		ID = iD;
	}

	public int getNumOfTuples()
	{
		return numOfTuples;
	}

	public void setNumOfTuples(int numOfTuples)
	{
		this.numOfTuples = numOfTuples;
	}

	public Page getNextPage()
	{
		return nextPage;
	}

	public void setNextPage(Page nextPage)
	{
		this.nextPage = nextPage;
	}

}
