package com.pinus.entity;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.pinus.api.annotation.DateTime;
import com.pinus.api.annotation.Field;
import com.pinus.api.annotation.Index;
import com.pinus.api.annotation.Indexes;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;

/**
 * 用户红心数据.
 * 
 * @author duanbn
 * 
 */
@Table(cluster = "redheart", name = "kaola_user_redheart", shardingBy = "userId", shardingNum = 25, cache = true)
@Indexes(value = { @Index(field = "uid,classId,columnId") })
public class KaolaUserRedHeart implements Serializable {

	private static final long serialVersionUID = 1L;

	@PrimaryKey(comment = "主键")
	private int id;

	@Field(comment = "用户主键")
	private int userId;

	@Field(length = 40, comment = "用户ID")
	private String uid;

	@Field(comment = "大类id")
	private int classId;

	@Field(comment = "节目id")
	private int columnId;

	@Field(length = 1, comment = "设备类型 常量：0(android),1(iphone)")
	private String deviceType;

	@Field(length = 100, comment = "客户端版本")
	private String version;

	@Field(comment = "期id")
	private int programId;

	@Field(length = 2, comment = "来源（1:导入的收藏, 0:导入的订阅, 2:纯红心节目）")
	private String source;

	@DateTime(comment = "创建时间")
	private Date createTime;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public int getClassId() {
		return classId;
	}

	public void setClassId(int classId) {
		this.classId = classId;
	}

	public int getColumnId() {
		return columnId;
	}

	public void setColumnId(int columnId) {
		this.columnId = columnId;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public int getProgramId() {
		return programId;
	}

	public void setProgramId(int programId) {
		this.programId = programId;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public int getId() {
		return id;
	}

}
