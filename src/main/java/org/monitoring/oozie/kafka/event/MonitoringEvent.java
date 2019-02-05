package org.monitoring.oozie.kafka.event;

import com.google.gson.annotations.SerializedName;

public class MonitoringEvent {
	@SerializedName("ts_start")
	private long tsStart;
	@SerializedName("ts_end")
	private long tsEnd;
	@SerializedName("ts_duration")
	private long tsDuration;
	private String status;
	@SerializedName("app_mon")
	private String appMon;
	private String severity;
	private String component;
	@SerializedName("job_name")
	private String jobName;
	@SerializedName("job_type")
	private String jobType;
	private String irt;
	private String trigram;
	private String entity;
	private String business;
	private String user;
	
	public String getAppMon() {
		return appMon;
	}
	public void setAppMon(String appMon) {
		this.appMon = appMon;
	}
	public long getTsStart() {
		return tsStart;
	}
	public void setTsStart(long tsStart) {
		this.tsStart = tsStart;
	}
	public long getTsEnd() {
		return tsEnd;
	}
	public void setTsEnd(long tsEnd) {
		this.tsEnd = tsEnd;
	}
	public long getTsDuration() {
		return tsDuration;
	}
	public void setTsDuration(long tsDuration) {
		this.tsDuration = tsDuration;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getSeverity() {
		return severity;
	}
	public void setSeverity(String severity) {
		this.severity = severity;
	}
	public String getComponent() {
		return component;
	}
	public void setComponent(String component) {
		this.component = component;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public String getJobType() {
		return jobType;
	}
	public void setJobType(String jobType) {
		this.jobType = jobType;
	}
	public String getIrt() {
		return irt;
	}
	public void setIrt(String irt) {
		this.irt = irt;
	}
	public String getTrigram() {
		return trigram;
	}
	public void setTrigram(String trigram) {
		this.trigram = trigram;
	}
	public String getEntity() {
		return entity;
	}
	public void setEntity(String entity) {
		this.entity = entity;
	}
	public String getBusiness() {
		return business;
	}
	public void setBusiness(String business) {
		this.business = business;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
}
