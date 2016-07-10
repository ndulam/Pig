package com.naresh.org;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Template for a basic Pig Loader
 */
public class XmlLoader extends LoadFunc {

    protected RecordReader reader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private String inputFormatClassName = null;
    private String loadLocation;
    private String columns;
    public XmlLoader(String columns) {
    	this.columns = columns;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        // if not manually set in options string
        if (inputFormatClassName == null) {
            if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
                inputFormatClassName = Bzip2TextInputFormat.class.getName();
            } else {
                inputFormatClassName = TextInputFormat.class.getName();
            }
        }
        try {
            return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
        } catch (InstantiationException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        }
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        reader = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            List values = new ArrayList();
            if (!reader.nextKeyValue()) {
                return null;
            }
            
            Text value = (Text) reader.getCurrentValue();
    		//String tag="TITLE|AUTHOR|COUNTRY|COMPANY|PRICE|YEAR";
    		String temp=value.toString();

    		String tags[] = columns.split("\\|");
    	     Tuple output = TupleFactory.getInstance().newTuple(6);
    		int i=0;
    		for(String str:tags)
    		{
    		String tagStart = "<"+str+">";
    		String tagEnd="</"+str+">";
    		System.out.println(temp);
    		System.out.println(tagStart);
    		System.out.println(tagEnd);
    		if(!tagStart.equalsIgnoreCase("<CATALOG>") ||!tagEnd.equalsIgnoreCase("</CATALOG>") || !tagStart.equalsIgnoreCase("<BOOK>") ||!tagEnd.equalsIgnoreCase("</BOOK>"))
    		{
    		String t = temp.substring(temp.indexOf(tagStart)+(tagStart.length()), temp.indexOf(tagEnd));
    		System.out.println(t);
    		output.set(i, t);
    		i++;
    		}
    		}
            return output;
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode,
                    PigException.REMOTE_ENVIRONMENT, e);
        }
    }

}