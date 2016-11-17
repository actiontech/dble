package io.mycat.route.function;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
public class PartitionBylongTest {
	@Rule
	public ExpectedException thrown= ExpectedException.none();
	@Test
	public void testCalculate1() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("2");
		rule.setPartitionLength("512");
		rule.init();
		String value ="0";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="511";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="512";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="1023";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="1024";
		Assert.assertEquals(true,0==rule.calculate(value));
	}
	 
	@Test
	public void testCalculate2() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("2");
		rule.setPartitionLength("1");
		rule.init();
		String value ="0";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="1";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="2";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="-1";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="-2";
		Assert.assertEquals(true,0==rule.calculate(value));
	} 
	@Test
	public void testCalculate3() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("3");
		rule.setPartitionLength("1");
		rule.init();
		String value ="0";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="1";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="2";
		Assert.assertEquals(true,2==rule.calculate(value)); 
		value ="3";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="-1";
		Assert.assertEquals(true,2==rule.calculate(value));
		value ="-2";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="-3";
		Assert.assertEquals(true,0==rule.calculate(value));
	}  
	
	
	@Test
	public void testCalculate4() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("1,2");
		rule.setPartitionLength("1,2");
		rule.init();
		String value ="0";
		Assert.assertEquals(true,0==rule.calculate(value));
		value ="1";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="2";
		Assert.assertEquals(true,1==rule.calculate(value));
		value ="3";
		Assert.assertEquals(true,2==rule.calculate(value));
		value ="4";
		Assert.assertEquals(true,2==rule.calculate(value));
		value ="5";
		Assert.assertEquals(true,0==rule.calculate(value)); 
	}
	
	@Test
	public void testCalculate5() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("3");
		rule.setPartitionLength("512");
		thrown.expect(RuntimeException.class); 
		rule.init();  
	}
	@Test
	public void testCalculate6() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("3");
		rule.setPartitionLength("1");
		rule.init();
		String value ="";
		thrown.expect(IllegalArgumentException.class); 
		rule.calculate(value); 
	} 
	@Test
	public void testCalculate7() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("-2");
		rule.setPartitionLength("1");
		thrown.expect(RuntimeException.class);
		rule.init(); 
		
	} 
	@Test
	public void testCalculate8() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("0");
		rule.setPartitionLength("1");
		thrown.expect(RuntimeException.class);
		rule.init(); 
		
	} 
	@Test
	public void testCalculate9() {
		PartitionByLong rule = new PartitionByLong();
		rule.setPartitionCount("1,-2");
		rule.setPartitionLength("1,4");
		thrown.expect(RuntimeException.class);
		rule.init(); 
		
	} 
}
