package org.pinus4j.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.pinus4j.api.FashionEntity;
import org.pinus4j.entity.annotations.DateTime;
import org.pinus4j.entity.annotations.Field;
import org.pinus4j.entity.annotations.PrimaryKey;
import org.pinus4j.entity.annotations.Table;
import org.pinus4j.entity.annotations.UpdateTime;

@Table(name = "test_entity", cluster = "pinus", shardingBy = "testInt", shardingNum = 3, cache = true)
public class TestEntity extends FashionEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @PrimaryKey(comment = "主键", isAutoIncrement = true)
    private Long              id;

    @Field(comment = "测试byte类型的字段")
    private byte              testByte;
    @Field
    private Byte              oTestByte;

    @Field
    private boolean           testBool;
    @Field
    private Boolean           oTestBool;

    @Field
    private char              testChar;
    @Field
    private Character         oTestChar;

    @Field
    private short             testShort;
    @Field
    private Short             oTestShort;

    @Field(name = "test_int")
    private int               testInt;
    @Field
    private Integer           oTestInt;

    @Field(name = "test_long")
    private long              testLong;
    @Field
    private Long              oTestLong;

    @Field
    private float             testFloat;
    @Field(isCanNull = true)
    private Float             oTestFloat;

    @Field
    private double            testDouble;
    @Field
    private Double            oTestDouble;

    @Field
    private String            testString;

    @DateTime(comment = "日期类型", name = "date_time")
    private Date              testDate;

    @UpdateTime(comment = "自动更新时间")
    private Timestamp         testTime;

    @Override
    public String toString() {
        return "TestEntity [id=" + id + ", testByte=" + testByte + ", oTestByte=" + oTestByte + ", testBool="
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
        TestEntity other = (TestEntity) obj;
        if (id != other.id)
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

    public Byte getOTestByte() {
        return oTestByte;
    }

    public void setOTestByte(Byte oTestByte) {
        this.oTestByte = oTestByte;
    }

    public boolean getTestBool() {
        return testBool;
    }

    public void setTestBool(boolean testBool) {
        this.testBool = testBool;
    }

    public Boolean getOTestBool() {
        return oTestBool;
    }

    public void setOTestBool(Boolean oTestBool) {
        this.oTestBool = oTestBool;
    }

    public char getTestChar() {
        return testChar;
    }

    public void setTestChar(char testChar) {
        this.testChar = testChar;
    }

    public Character getOTestChar() {
        return oTestChar;
    }

    public void setOTestChar(Character oTestChar) {
        this.oTestChar = oTestChar;
    }

    public short getTestShort() {
        return testShort;
    }

    public void setTestShort(short testShort) {
        this.testShort = testShort;
    }

    public Short getOTestShort() {
        return oTestShort;
    }

    public void setOTestShort(Short oTestShort) {
        this.oTestShort = oTestShort;
    }

    public int getTestInt() {
        return testInt;
    }

    public void setTestInt(int testInt) {
        this.testInt = testInt;
    }

    public Integer getOTestInt() {
        return oTestInt;
    }

    public void setOTestInt(Integer oTestInt) {
        this.oTestInt = oTestInt;
    }

    public long getTestLong() {
        return testLong;
    }

    public void setTestLong(long testLong) {
        this.testLong = testLong;
    }

    public Long getOTestLong() {
        return oTestLong;
    }

    public void setOTestLong(Long oTestLong) {
        this.oTestLong = oTestLong;
    }

    public float getTestFloat() {
        return testFloat;
    }

    public void setTestFloat(float testFloat) {
        this.testFloat = testFloat;
    }

    public Float getOTestFloat() {
        return oTestFloat;
    }

    public void setOTestFloat(Float oTestFloat) {
        this.oTestFloat = oTestFloat;
    }

    public double getTestDouble() {
        return testDouble;
    }

    public void setTestDouble(double testDouble) {
        this.testDouble = testDouble;
    }

    public Double getOTestDouble() {
        return oTestDouble;
    }

    public void setOTestDouble(Double oTestDouble) {
        this.oTestDouble = oTestDouble;
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
