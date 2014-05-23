package com.pinus;

import java.io.Serializable;
import java.util.Date;

import com.pinus.api.IGlobalEntity;
import com.pinus.api.annotation.DateTime;
import com.pinus.api.annotation.Field;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;

@Table(cluster = "klstorage", cache = true)
public class TestGlobalEntity implements Serializable, IGlobalEntity {

	private static final long serialVersionUID = 1L;

	@PrimaryKey
	private long id;

	@Field
	private byte testByte;

	@Field
	private boolean testBool;

	@Field
	private char testChar;

	@Field
	private short testShort;

	@Field
	private int testInt;

	@Field
	private long testLong;

	@Field
	private float testFloat;

	@Field
	private double testDouble;

	@Field
	private String testString;

	@DateTime
	private Date testDate;

	@Override
	public String getClusterName() {
		return "klstorage";
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public byte getTestByte() {
		return testByte;
	}

	public void setTestByte(byte testByte) {
		this.testByte = testByte;
	}

	public boolean isTestBool() {
		return testBool;
	}

	public void setTestBool(boolean testBool) {
		this.testBool = testBool;
	}

	public char getTestChar() {
		return testChar;
	}

	public void setTestChar(char testChar) {
		this.testChar = testChar;
	}

	public short getTestShort() {
		return testShort;
	}

	public void setTestShort(short testShort) {
		this.testShort = testShort;
	}

	public int getTestInt() {
		return testInt;
	}

	public void setTestInt(int testInt) {
		this.testInt = testInt;
	}

	public long getTestLong() {
		return testLong;
	}

	public void setTestLong(long testLong) {
		this.testLong = testLong;
	}

	public float getTestFloat() {
		return testFloat;
	}

	public void setTestFloat(float testFloat) {
		this.testFloat = testFloat;
	}

	public double getTestDouble() {
		return testDouble;
	}

	public void setTestDouble(double testDouble) {
		this.testDouble = testDouble;
	}

	public String getTestString() {
		return testString;
	}

	public void setTestString(String testString) {
		this.testString = testString;
	}

	public Date getTestDate() {
		return testDate;
	}

	public void setTestDate(Date testDate) {
		this.testDate = testDate;
	}

}
