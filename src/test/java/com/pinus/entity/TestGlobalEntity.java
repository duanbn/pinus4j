package com.pinus.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.pinus.api.FashionEntity;
import org.pinus.api.annotation.DateTime;
import org.pinus.api.annotation.Field;
import org.pinus.api.annotation.Index;
import org.pinus.api.annotation.Indexes;
import org.pinus.api.annotation.PrimaryKey;
import org.pinus.api.annotation.Table;
import org.pinus.api.annotation.UpdateTime;

@Table(cluster = "pinus", cache = true)
@Indexes({ @Index(field = "testInt", isUnique = true) })
public class TestGlobalEntity extends FashionEntity implements Serializable {

	private static final long serialVersionUID = 1L;

	@PrimaryKey(comment = "主键")
	private long id;

	@Field
	private byte testByte;
	@Field
	private Byte oTestByte;

	@Field
	private boolean testBool;
	@Field
	private Boolean oTestBool;

	@Field
	private char testChar;
	@Field
	private Character oTestChar;

	@Field
	private short testShort;
	@Field
	private Short oTestShort;

	@Field
	private int testInt;
	@Field
	private Integer oTestInt;

	@Field
	private long testLong;
	@Field
	private Long oTestLong;

	@Field
	private float testFloat;
	@Field
	private Float oTestFloat;

	@Field
	private double testDouble;
	@Field
	private Double oTestDouble;

	@Field
	private String testString;

	@DateTime
	private Date testDate;

	@UpdateTime(comment = "自动更新时间")
	private Timestamp testTime;

	@Override
	public String toString() {
		return "TestGlobalEntity [id=" + id + ", testByte=" + testByte + ", oTestByte=" + oTestByte + ", testBool="
				+ testBool + ", oTestBool=" + oTestBool + ", testChar=" + testChar + ", oTestChar=" + oTestChar
				+ ", testShort=" + testShort + ", oTestShort=" + oTestShort + ", testInt=" + testInt + ", oTestInt="
				+ oTestInt + ", testLong=" + testLong + ", oTestLong=" + oTestLong + ", testFloat=" + testFloat
				+ ", oTestFloat=" + oTestFloat + ", testDouble=" + testDouble + ", oTestDouble=" + oTestDouble
				+ ", testString=" + testString + ", testDate=" + testDate + ", testTime=" + testTime + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((oTestBool == null) ? 0 : oTestBool.hashCode());
		result = prime * result + ((oTestByte == null) ? 0 : oTestByte.hashCode());
		result = prime * result + ((oTestChar == null) ? 0 : oTestChar.hashCode());
		result = prime * result + ((oTestDouble == null) ? 0 : oTestDouble.hashCode());
		result = prime * result + ((oTestFloat == null) ? 0 : oTestFloat.hashCode());
		result = prime * result + ((oTestInt == null) ? 0 : oTestInt.hashCode());
		result = prime * result + ((oTestLong == null) ? 0 : oTestLong.hashCode());
		result = prime * result + ((oTestShort == null) ? 0 : oTestShort.hashCode());
		result = prime * result + (testBool ? 1231 : 1237);
		result = prime * result + testByte;
		result = prime * result + testChar;
		long temp;
		temp = Double.doubleToLongBits(testDouble);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + Float.floatToIntBits(testFloat);
		result = prime * result + testInt;
		result = prime * result + (int) (testLong ^ (testLong >>> 32));
		result = prime * result + testShort;
		result = prime * result + ((testString == null) ? 0 : testString.hashCode());
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
		if (oTestBool == null) {
			if (other.oTestBool != null)
				return false;
		} else if (!oTestBool.equals(other.oTestBool))
			return false;
		if (oTestByte == null) {
			if (other.oTestByte != null)
				return false;
		} else if (!oTestByte.equals(other.oTestByte))
			return false;
		if (oTestChar == null) {
			if (other.oTestChar != null)
				return false;
		} else if (!oTestChar.equals(other.oTestChar))
			return false;
		if (oTestDouble == null) {
			if (other.oTestDouble != null)
				return false;
		} else if (!oTestDouble.equals(other.oTestDouble))
			return false;
		if (oTestFloat == null) {
			if (other.oTestFloat != null)
				return false;
		} else if (!oTestFloat.equals(other.oTestFloat))
			return false;
		if (oTestInt == null) {
			if (other.oTestInt != null)
				return false;
		} else if (!oTestInt.equals(other.oTestInt))
			return false;
		if (oTestLong == null) {
			if (other.oTestLong != null)
				return false;
		} else if (!oTestLong.equals(other.oTestLong))
			return false;
		if (oTestShort == null) {
			if (other.oTestShort != null)
				return false;
		} else if (!oTestShort.equals(other.oTestShort))
			return false;
		if (testBool != other.testBool)
			return false;
		if (testByte != other.testByte)
			return false;
		if (testChar != other.testChar)
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

	public Byte getoTestByte() {
		return oTestByte;
	}

	public void setoTestByte(Byte oTestByte) {
		this.oTestByte = oTestByte;
	}

	public Boolean getoTestBool() {
		return oTestBool;
	}

	public void setoTestBool(Boolean oTestBool) {
		this.oTestBool = oTestBool;
	}

	public Character getoTestChar() {
		return oTestChar;
	}

	public void setoTestChar(Character oTestChar) {
		this.oTestChar = oTestChar;
	}

	public Short getoTestShort() {
		return oTestShort;
	}

	public void setoTestShort(Short oTestShort) {
		this.oTestShort = oTestShort;
	}

	public Integer getoTestInt() {
		return oTestInt;
	}

	public void setoTestInt(Integer oTestInt) {
		this.oTestInt = oTestInt;
	}

	public Long getoTestLong() {
		return oTestLong;
	}

	public void setoTestLong(Long oTestLong) {
		this.oTestLong = oTestLong;
	}

	public Float getoTestFloat() {
		return oTestFloat;
	}

	public void setoTestFloat(Float oTestFloat) {
		this.oTestFloat = oTestFloat;
	}

	public Double getoTestDouble() {
		return oTestDouble;
	}

	public void setoTestDouble(Double oTestDouble) {
		this.oTestDouble = oTestDouble;
	}

}
