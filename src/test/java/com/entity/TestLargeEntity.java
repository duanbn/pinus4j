package com.entity;

import java.io.Serializable;

import com.pinus.api.annotation.Field;
import com.pinus.api.annotation.Index;
import com.pinus.api.annotation.Indexes;
import com.pinus.api.annotation.PrimaryKey;
import com.pinus.api.annotation.Table;

@Table(cluster = "klstorage")
@Indexes(@Index(field = "userName,password", isUnique = true))
public class TestLargeEntity implements Serializable {

	private static final long serialVersionUID = 1L;

	@PrimaryKey
	private long id;

	@Field
	private String uuid;

	@Field
	private String userName;

	@Field
	private String password;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
