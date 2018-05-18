package io.xh.xtoolcabinet.component.queue;

import java.util.Date;

public class MsgPo {

	private long id;
	private String bizKey;
	private String topic;
	private String msgContent;
	private int retriedTimes;
	private Date nextRetryTime;

	private int locked;

	private Date createTime;

	private Date updateTime;


	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getBizKey() {
		return bizKey;
	}

	public void setBizKey(String bizKey) {
		this.bizKey = bizKey;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getMsgContent() {
		return msgContent;
	}

	public void setMsgContent(String msgContent) {
		this.msgContent = msgContent;
	}

	public int getRetriedTimes() {
		return retriedTimes;
	}

	public void setRetriedTimes(int retriedTimes) {
		this.retriedTimes = retriedTimes;
	}

	public Date getNextRetryTime() {
		return nextRetryTime;
	}

	public void setNextRetryTime(Date nextRetryTime) {
		this.nextRetryTime = nextRetryTime;
	}

	public int getLocked() {
		return locked;
	}

	public void setLocked(int locked) {
		this.locked = locked;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	@Override
	public String toString() {
		return "MsgPo{" +
				"id=" + id +
				", bizKey='" + bizKey + '\'' +
				", topic='" + topic + '\'' +
				", msgContent='" + msgContent + '\'' +
				", retriedTimes=" + retriedTimes +
				", nextRetryTime=" + nextRetryTime +
				", locked=" + locked +
				", createTime=" + createTime +
				", updateTime=" + updateTime +
				'}';
	}
}
