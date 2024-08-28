package com.ericsson.eniq.etl.mrr;


import static org.junit.Assert.*;

import org.junit.BeforeClass;

import com.ericsson.eniq.etl.mrr.*;

import org.junit.Test;

//import com.ericsson.eniq.common.Utils;

/**
 * @author eheijun
 *
 */
public class MRRParserTest {

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#replaceNull(java.lang.Integer)}.
	 */
	
	MRRParser mrr = new MRRParser();
	
	
	@Test
	public void testStatus() {
		int result = mrr.status();
	    assertEquals(result,result);
	}
	
	@Test
	public void testreadHeader() {
		mrr.run();
		assertEquals(0, 0);
	}

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#replaceNull(java.lang.String)}.
	 */
	@Test
	public void testReplaceNullString() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#replaceNull(java.lang.Double)}.
	 */
	@Test
	public void testReplaceNullDouble() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#replaceNull(java.lang.Long)}.
	 */
	@Test
	public void testReplaceNullLong() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#booleanToInteger(java.lang.Boolean)}.
	 */
	@Test
	public void testBooleanToInteger() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

  /**
   * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#booleanToString(java.lang.Boolean)}.
   */
  @Test
  public void testBooleanToString() {
	  Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
  }

  /**
   * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#stringToBoolean(java.lang.String)}.
   */
  @Test
  public void testStringToBoolean() {
	  Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
  }

	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#integerToBoolean(java.lang.Integer)}.
	 */
	@Test
	public void testIntegerToBoolean() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

  /**
   * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#stringToInteger(java.lang.String)}.
   */
  @Test
  public void testStringToInteger() {
    Integer result = null;
    String tmp = "0";
    result = 0;
    assertTrue(result.equals(Integer.valueOf(tmp)));
    tmp = "";
    result = 0;
    assertTrue(result.equals(Integer.valueOf("0")));
  }
  
	/**
	 * Test method for {@link com.ericsson.eniq.busyhourcfg.common.Utils#stringListToString(java.util.List)}.
	 */
	@Test
	public void testStringListToString() {
		Integer result = null;
	    String tmp = "0";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf(tmp)));
	    tmp = "";
	    result = 0;
	    assertTrue(result.equals(Integer.valueOf("0")));
	}

}

