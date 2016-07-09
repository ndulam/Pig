/*Pig UDF to validate the records. input to the UDF is each record and columns name and types seperated by PIPE */
package com.pig.udf;
import java.io.IOException;
import org.apache.commons.validator.GenericValidator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class RecordValidator extends EvalFunc<String> {
	
	@Override
    public Schema outputSchema (Schema input) {
        if(input.size()!=2){
            throw new IllegalArgumentException("input should contains two elements!");
        }
 
        return new Schema(new FieldSchema("RecordValidator",DataType.CHARARRAY));
    }
 
    private String populateSet(String dataBag1,String dataBag2){
    	
           String data_record =  dataBag1 ;
           String header_record =  dataBag2 ;
           
           String[] dataarray = data_record.toString().split("\\|", -1) ;
           String[] headerarray = header_record.toString().split("\\|", -1) ;
           
           String[] columnsarray = new String[headerarray.length] ;
           String[] columntypesarray = new String[headerarray.length] ;
        
           String final_data_record ="";
            
           if(dataarray.length == headerarray.length) {
        	  
        // Audit columns 
        	   
        	   String Audit_Flag  =  "";
        	   String Audit_Error =  "";
        	   
        	   
        	   
        	   for(int i=0; i < headerarray.length ; i++) {
        		 String[] header_split  = headerarray[i].split(" ", -1) ;
        		 
        		 columnsarray[i] = header_split[0].trim() ;
        		 columntypesarray[i] = header_split[1].trim();
        	
        	     
        		 if(columntypesarray[i].toUpperCase().contains("INT")) {
        			 if (!isInteger(dataarray[i])) {
        				 Audit_Flag = "Error";
        				 Audit_Error += dataarray[i] + " is not an integer - " ;
						}
        		 }else if (columntypesarray[i].toUpperCase().contains("TIMESTAMP")) {

						if (!isTimeStamp(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a valid timestamp -" ;
						}
					} else if (columntypesarray[i].toUpperCase().contains("TIME")) {

						if (!isTime(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a valid time -" ;
						}
					}

					else if (columntypesarray[i].toUpperCase().contains("DECIMAL")) {

						if (!isDecimal(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a decimal - " ;
						}
					} else if (columntypesarray[i].toUpperCase().contains("FLOAT")) {

						if (!isDecimal(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a float - " ;
						}
					} else if (columntypesarray[i].toUpperCase().contains("REAL")) {

						if (!isDecimal(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a real num - " ;
						}
					} else if (columntypesarray[i].toUpperCase().contains("DATE")) {
						if (!isDate(dataarray[i])) {
							Audit_Flag = "Error";
	        				 Audit_Error += dataarray[i] + " is not a valid date - " ;
						}
					}

        		
      	 }
        	   
        	 // Include Audit columns in data record  
             
        	        final_data_record  = Audit_Flag + "|" + Audit_Error + "|" + data_record.toString() ; 
        	 
            }
           else {
        	   
        	   System.out.println("Number of data fields : " + dataarray.length + " is not same as "
        	   		+ "number of header columns : " + headerarray.length ) ;
               
        	   final_data_record  = "Error" + "|" + "Number of Data fields mismatch" + "|" + data_record.toString() ;
           
           }
        	   
        return final_data_record ;
    }
    
    
    public static boolean isDecimal(String str) {
		return str.matches("^-?[0-9]+(\\.[0-9]+)?$");
	}

	public static boolean isInteger(String str) {
		return str.matches("^-?[0-9]+?$");
	}

	public static boolean isDate(String str) {
		return GenericValidator.isDate(str, "yyyy-MM-dd", true) || GenericValidator.isDate(str, "yyyy/MM/dd", true)
				|| GenericValidator.isDate(str, "MM/dd/yyyy", true) || GenericValidator.isDate(str, "MM-dd-yyyy", true)
				|| GenericValidator.isDate(str, "yyyyMMdd", true)
				|| GenericValidator.isDate(str, "MMddyyyy", true);
	}

	public static boolean isTime(String str) {
		return GenericValidator.isDate(str, "HH:mm:ss", true) || GenericValidator.isDate(str, "HHmmss", true);
	}

	public static boolean isTimeStamp(String str) {
		boolean retVal;
		try {
			retVal = GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss.S", true)
					|| GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss.SS", true)
					|| GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss.SSS", true)
					|| GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss.SSSSSS", true)
					|| GenericValidator.isDate(str, "yyyy-MM-dd:HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd:HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss.S", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss.SS", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss.SSS", true)
					|| GenericValidator.isDate(str, "MM/dd/yyyy HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss.SSSSSS", true)
					|| GenericValidator.isDate(str, "MM-dd-yyyy:HH:mm:SS", true)
					|| GenericValidator.isDate(str, "MM/dd/yyyy:HH:mm:SS", true)					
					|| GenericValidator.isDate(str, "yyyyMMdd HH:mm:ss", true)
					|| GenericValidator.isDate(str, "yyyyMMdd HH:mm:ss.S", true)
					|| GenericValidator.isDate(str, "yyyyMMdd HH:mm:ss.SS", true)
					|| GenericValidator.isDate(str, "yyyyMMdd HH:mm:ss.SSS", true)
					|| GenericValidator.isDate(str, "yyyyMMdd HH:mm:ss.SSSSSS", true)
					|| GenericValidator.isDate(str, "yyyyMMdd:HH:mm:ss", true);
	
			if (!retVal) {
				if (str.length() == 26) {
					str = str.substring(0, 19);
					retVal = GenericValidator.isDate(str, "yyyy-MM-dd HH:mm:ss", true)
							|| GenericValidator.isDate(str, "yyyy/MM/dd HH:mm:ss", true);
				}
			}

		} catch (Exception e) {
			retVal = false;
		}
		return retVal;

	}    @Override
    public String exec(Tuple input) throws IOException {
 
    	String output = populateSet((String) input.get(0),(String) input.get(1));
       
    	//Set setB = populateSet((DataBag) input.get(1));
        
    	return output ;
 
    }
}
