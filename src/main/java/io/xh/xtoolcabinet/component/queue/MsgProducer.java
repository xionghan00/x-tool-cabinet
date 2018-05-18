package io.xh.xtoolcabinet.component.queue;

import java.util.Date;

public class MsgProducer {

	private MsgQueueService msgQueueService;

	public MsgProducer(MsgQueueService msgQueueService) {
		this.msgQueueService = msgQueueService;
	}

	public void produce(String topic, String content, String bizKey) {
		produce(topic, content, bizKey, new Date());
	}

	public void produce(String topic, String content) {
		produce(topic, content, "");
	}

	public void produce(String topic, String content, String bizKey, Date scheduleTime) {
		MsgPo msgPo = new MsgPo();

		msgPo.setTopic(topic);
		msgPo.setMsgContent(content);
		msgPo.setLocked(0);
		msgPo.setBizKey(bizKey);

		Date now = new Date();
		msgPo.setCreateTime(now);
		msgPo.setUpdateTime(now);
		msgPo.setRetriedTimes(0);
		msgPo.setNextRetryTime(scheduleTime);

		msgQueueService.insertMsg(msgPo);
	}
}
