package com.naresh.org;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;;
public class ConcatingUDF extends EvalFunc<Tuple>
{
	private static Pattern DELIM_PATTERN = Pattern.compile("[\\s,]+");
	public Schema outputSchema(Schema arg0) 
	{
		Schema tupleSchema = new Schema(); 
		tupleSchema.add(new FieldSchema("firstName", DataType.CHARARRAY));
		return new Schema(new FieldSchema("ConcatedName",tupleSchema));
	}

	public Tuple exec(Tuple input) throws IOException 
	{
		if(input==null || input.size()!=2)
		{
           throw new IOException("Invalid input. Please pass in both a list of column names and the columns themse" +
"lves.");
		}
		System.out.println(2);
		String columnselector = input.get(0).toString();
		System.out.println(3);
		DataBag rows = (DataBag) input.get(1);
		System.out.println(4);
		String[] selections =DELIM_PATTERN.split(columnselector);
		System.out.println(5);
		Tuple output = TupleFactory.getInstance().newTuple(selections.length);
		System.out.println(6);
		Iterator<Tuple> it =rows.iterator();
		System.out.println(7);
		String names="";
		int count =7;
		while(it.hasNext())
		{
			Tuple t = it.next();
			System.out.println("iterate"+(count++));
			String temp = t.get(1).toString();
			System.out.println(count++);
			names=names+" "+temp;
		}
		System.out.println("before");
		output.set(0,names);
		System.out.println("after");
		return output;
	}
}
