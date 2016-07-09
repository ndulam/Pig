/**
 * Converting the iput date to from input format to output format 
 *
 */
package com.transformation.udf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class DateConvertor extends EvalFunc<String> {

	public String exec(Tuple input) throws ExecException {

		String dateText = (String) input.get(0);
		String inputDatePatternText = (String) input.get(1);
		String outputDatePatternText = (String) input.get(2);

		return evaluate(dateText, inputDatePatternText, outputDatePatternText);
	}

	// Default time zone.
	private final SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/** This method not taking any argument as a parameter and create an object
	* of date class, returning current system date in default format
	* "yyyy-MM-dd HH:mm:ss".
	*/
	public String evaluate() {
		Date date = new Date();
		return evaluate(date);
	}

	/** This method taking one argument ‘date’ as a parameter and returning
	* date in default format "yyyy-MM-dd HH:mm:ss".
	*/
	public String evaluate(Date date) {
		String outFormat = formatter.format(date);
		return outFormat;
	}

	/** This method taking two argument ‘date, outputDatePatternText’ as a
	* parameter. Here default format get changed by outputDatePatternText
	* provided by user which returns date in outputDatePatternText format.
	*/
	public String evaluate(Date date, String outputDatePatternText) {
		if (date == null) {
			return null;
		}
		try {
			String outFormat = null;
			formatter.applyPattern(outputDatePatternText);
			outFormat = formatter.format(date);
			return outFormat;
		} catch (Exception e) {
			return null;
		}
	}

	/** This method taking three arguments ‘dateText, inputDatePatternText,
	* outputDatePatternText’ as a parameter. Here dateText get changed in
	* date object in proper format using inputDatePatternText and default
	* format get changed by outputDatePatternText provided by user which
	* returns date in outputDatePatternText format.
	*/
	public String evaluate(String dateText, String inputDatePatternText,
			String outputDatePatternText) {

		if (dateText == null || inputDatePatternText == null) {
			return null;
		}

		// If input string contains AM/PM then set into
		try {
			formatter.applyPattern(inputDatePatternText);
			Date date = formatter.parse(dateText);
			if (dateText.contains("AM") && date.getHours() >= 12) {
				date.setHours(date.getHours() - 12);
			} else if (dateText.contains("PM") && date.getHours() < 12) {
				date.setHours(date.getHours() + 12);
			}
			return evaluate(date, outputDatePatternText);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public String evaluateToAMPM(Date date, String outputDatePatternText) {
		if (date == null) {
			return null;
		}
		try {
			String outFormat = null;
			formatter.applyPattern(outputDatePatternText);
			if (outputDatePatternText.contains("AM")
					|| outputDatePatternText.contains("PM")) {
				if (date.getHours() >= 12) {
					if (date.getHours() > 12) {
						date.setHours(date.getHours() - 12);
					}
					// outFormat = formatter.format(date) + " PM";
					outFormat = formatter.format(date) + " PM";
				} else {
					outFormat = formatter.format(date) + " AM";
				}
			} else {
				outFormat = formatter.format(date);
			}
			return outFormat;
		} catch (Exception e) {
			return null;
		}
	}

	public String evaluateToAMPMDate(String dateText,
			String inputDatePatternText, String outputDatePatternText) {

		if (dateText == null || inputDatePatternText == null) {
			return null;
		}
		if (dateText.contains("AM") || dateText.contains("PM")) {
			try {
				formatter.applyPattern(inputDatePatternText);
				Date date = formatter.parse(dateText);
				if (dateText.contains("AM") && date.getHours() >= 12) {
					date.setHours(date.getHours() - 12);
				} else if (dateText.contains("PM") && date.getHours() < 12) {
					date.setHours(date.getHours() + 12);
				}
				return evaluate(date, outputDatePatternText);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}

			// return dateText;
		}
		try {
			formatter.applyPattern(inputDatePatternText);
			Date date = formatter.parse(dateText);
			return evaluateToAMPM(date, outputDatePatternText);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}