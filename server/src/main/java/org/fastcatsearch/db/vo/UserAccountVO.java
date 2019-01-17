package org.fastcatsearch.db.vo;

import org.fastcatsearch.util.MessageDigestUtils;
import java.util.Date;

public class UserAccountVO {
	public static final String ADMIN_USER_NAME = "Administrator";
	public static final String ADMIN_USER_ID = "admin";
	
	public int id;
	public String name;
	public String userId;
	public String password;
	public String email;
	public String sms;
	public String telegram;
	public int groupId;
	public int loginFailCount;
	public Date loginSuccessDatetime;
	public Date loginFailDatetime;
	
	public UserAccountVO(){ }
	
	public UserAccountVO(String name, String userId, String password, String email, String sms, int groupId) {
		this.name = name;
		this.userId = userId;
		this.email = email;
		this.sms = sms;
		this.telegram = "";
		this.groupId = groupId;
		this.loginFailCount = 0;
		
		setEncryptedPassword(password);
	}

	public UserAccountVO(String name, String userId, String password, String email, String sms, String telegram, int groupId) {
		this.name = name;
		this.userId = userId;
		this.email = email;
		this.sms = sms;
		this.telegram = telegram;
		this.groupId = groupId;
		this.loginFailCount = 0;

		setEncryptedPassword(password);
	}
	
	public void setEncryptedPassword(String password){
		this.password = MessageDigestUtils.getSHA1String(password);
	}
	
	public boolean isEqualsEncryptedPassword(String password){
		return this.password.equalsIgnoreCase(MessageDigestUtils.getSHA1String(password));
	}
}
