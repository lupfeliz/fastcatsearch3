package org.fastcatsearch.db.vo;

import java.sql.Timestamp;

public class SystemMonitoringInfoVO {
	public int id;
	public int cpu;
	public int mem;
	public double load;
	public Timestamp when;
	public String type;
}
