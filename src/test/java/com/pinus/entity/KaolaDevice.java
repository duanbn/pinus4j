package com.pinus.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.pinus.api.annotation.DateTime;
import com.pinus.api.annotation.Field;
import com.pinus.api.annotation.Index;
import com.pinus.api.annotation.Indexes;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;
import com.pinus.api.annotation.UpdateTime;

/**
 * 用户设备.
 * 
 * @author duanbn
 * 
 */
@Table(cluster = "user", name = "kaola_device", shardingBy="udid", shardingNum = 25, cache = true)
@Indexes({ @Index(field = "udid", isUnique = true) })
public class KaolaDevice implements Serializable {

	private static final long serialVersionUID = 1L;

	@PrimaryKey(comment = "主键")
	private int id;

	@Field(length = 40, comment = "设备ID")
	private String udid;

	@Field(length = 1, comment = "设备类型 0安卓，1苹果")
	private String deviceType;

	@Field(length = 40, comment = "渠道号")
	private String channel;

	@Field(length = 20, comment = "版本号")
	private String version;

	@DateTime(comment = "创建时间")
	private Date createTime = new Date();

	@UpdateTime(comment = "更新时间")
	private Timestamp updateTime;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public String getClusterName() {
		return "user";
	}

	public String getShardingValue() {
		return this.udid;
	}

	public int getId() {
		return id;
	}

	public String getUdid() {
		return udid;
	}

	public void setUdid(String udid) {
		this.udid = udid;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setUpdateTime(Timestamp updateTime) {
		this.updateTime = updateTime;
	}

	public Timestamp getUpdateTime() {
		return updateTime;
	}

}
