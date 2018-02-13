package project;
import java.io.FileWriter;
import java.io.IOException;

public class Page
{
	private int ID = 0;
	private int numOfTuples;
	private Page nextPage;
	private String ownerTable;

	public Page(String ownerTable, int ID)
	{
		this.ID = ID;
		this.numOfTuples = 0;
		this.nextPage = null;
		this.ownerTable = ownerTable;

		try
		{

			FileWriter fw = new FileWriter("src/DB2App/" + ownerTable + " Table/Page " + ID + ".ser", true);

		} catch (IOException i)
		{
			System.out.println("Could Not Create a Page!");
		}
	}

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
