package org.pinus4j.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.pinus4j.entity.annotations.DateTime;
import org.pinus4j.entity.annotations.Field;
import org.pinus4j.entity.annotations.Index;
import org.pinus4j.entity.annotations.Indexes;
import org.pinus4j.entity.annotations.PrimaryKey;
import org.pinus4j.entity.annotations.Table;
import org.pinus4j.entity.annotations.UpdateTime;

@Table(cluster = "pinus", cache = true)
@Indexes({ @Index(field = "testInt,oTestBool") })
public class TestGlobalUnionKeyEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    @PrimaryKey(comment = "主键")
    private String            id;

    @PrimaryKey
    private byte              testByte;
    @Field
    private Byte              oTestByte;

    @Field(name = "test_bool")
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

    @Field
    private int               testInt;
    @Field
    private Integer           oTestInt;

    @Field
    private long              testLong;
    @Field
    private Long              oTestLong;

    @Field
    private float             testFloat;
    @Field
    private Float             oTestFloat;

    @Field
    private double            testDouble;
    @Field
    private Double            oTestDouble;

    @Field
    private String            testString;

    @Field
    private String            index;

    @DateTime
    private Date              testDate;

    @UpdateTime(comment = "自动更新时间")
    private Timestamp         testTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
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

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
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
