package project;

import java.util.LinkedList;

public class Tuple implements java.io.Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private LinkedList<Object> tupleInfo;
	private int pageID;

	public Tuple(LinkedList<Object> tupleInfo)
	{
		this.tupleInfo = tupleInfo;
	}

	public int getPageID()
	{
		return this.pageID;
	}

	public void setPageID(int p)
	{
		this.pageID = p;
	}

	public LinkedList<Object> getTupleInfo()
	{
		return this.tupleInfo;
	}

	public String toString()
	{
		String s = "";
		for (int i = 0; i < tupleInfo.size(); i++)
		{
			s += tupleInfo.get(i);
			if (i != tupleInfo.size() - 1)
				s += ", ";
		}
		return s;
	}

}
