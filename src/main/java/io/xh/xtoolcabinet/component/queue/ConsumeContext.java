package io.xh.xtoolcabinet.component.queue;


public class ConsumeContext {

	MsgPo msgPo;

	public ConsumeContext(MsgPo po) {
		this.msgPo = po;
	}

	public Long getTaskId() {
		return msgPo.getId();
	}

	public String getTopic() {
		return msgPo.getTopic();
	}

	public String getContent() {
		return msgPo.getMsgContent();
	}

	public int getRetriedTimes() {
		return msgPo.getRetriedTimes();
	}


	@Override
	public String toString() {
		if (null == msgPo) {
			return "ConsumeContext{MsgPo{}}";
		}
		return "ConsumeContext{" + msgPo.toString() + "}";
	}
}
