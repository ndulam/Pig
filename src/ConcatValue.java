/**
 * UDF to concat the values provided to UDF
 *
 */
package com.transformation.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class ConcatValue extends EvalFunc<String> {

	public String exec(Tuple input) throws ExecException {

		StringBuilder concatValue = new StringBuilder();

		int size = input.size();
		for (int i = 0; i < size; i++) {

			if (StringUtils.isEmpty((String) input.get(i)))
				concatValue.append("");
			else
				concatValue.append((String) input.get(i));
		}
		return concatValue.toString();
	}
}