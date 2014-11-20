package com.pinus.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import com.pinus.api.FashionEntity;
import com.pinus.api.annotation.DateTime;
import com.pinus.api.annotation.Field;
import com.pinus.api.annotation.Index;
import com.pinus.api.annotation.Indexes;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;
import com.pinus.api.annotation.UpdateTime;

@Table(cluster = "pinus", cache = true)
@Indexes({ @Index(field = "testInt", isUnique = true) })
public class TestGlobalEntity extends FashionEntity implements Serializable {

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

	@UpdateTime(comment = "自动更新时间")
	private Timestamp testTime;

	@Override
	public String toString() {
		return "TestGlobalEntity [id=" + id + ", testByte=" + testByte + ", testBool=" + testBool + ", testChar="
				+ testChar + ", testShort=" + testShort + ", testInt=" + testInt + ", testLong=" + testLong
				+ ", testFloat=" + testFloat + ", testDouble=" + testDouble + ", testString=" + testString
				+ ", testDate=" + testDate + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + (testBool ? 1231 : 1237);
		result = prime * result + testByte;
		result = prime * result + testChar;
		result = prime * result + ((testDate == null) ? 0 : testDate.hashCode());
		long temp;
		temp = Double.doubleToLongBits(testDouble);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + Float.floatToIntBits(testFloat);
		result = prime * result + testInt;
		result = prime * result + (int) (testLong ^ (testLong >>> 32));
		result = prime * result + testShort;
		result = prime * result + ((testString == null) ? 0 : testString.hashCode());
		result = prime * result + ((testTime == null) ? 0 : testTime.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TestGlobalEntity other = (TestGlobalEntity) obj;
		if (id != other.id)
			return false;
		if (testBool != other.testBool)
			return false;
		if (testByte != other.testByte)
			return false;
		if (testChar != other.testChar)
			return false;
		if (testDate == null) {
			if (other.testDate != null)
				return false;
		} else if (!testDate.equals(other.testDate))
			return false;
		if (Double.doubleToLongBits(testDouble) != Double.doubleToLongBits(other.testDouble))
			return false;
		if (Float.floatToIntBits(testFloat) != Float.floatToIntBits(other.testFloat))
			return false;
		if (testInt != other.testInt)
			return false;
		if (testLong != other.testLong)
			return false;
		if (testShort != other.testShort)
			return false;
		if (testString == null) {
			if (other.testString != null)
				return false;
		} else if (!testString.equals(other.testString))
			return false;
		if (testTime == null) {
			if (other.testTime != null)
				return false;
		} else if (!testTime.equals(other.testTime))
			return false;
		return true;
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

	public Timestamp getTestTime() {
		return testTime;
	}

	public void setTestTime(Timestamp testTime) {
		this.testTime = testTime;
	}

}
