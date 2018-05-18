//package io.xh.xtoolcabinet.component.queue;
//
//import com.google.common.collect.ImmutableMap;
//import com.qf.ndes.dao.MsgQueueMapper;
//import com.qf.ndes.entity.MsgPo;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//
//import java.util.Date;
//import java.util.List;
//
//public class MsgQueueServiceImpl implements MsgQueueService{
//
//	public static final Date UNREACHABLE_TIME = new Date(253370736000000L);
//
//	// 30 min
//	public static final long MSG_MAX_LOCK_DURATION = 1000L * 60 * 30;
//
//	// 2 day
//	public static final long ABORT_MSG_MAX_DURATION = 1000L * 60 * 60 * 24 * 2;
//
//	public static int[] RETRY_GAP = new int[]{
//			10, 30, 60, 300, 1800, 3600
//	};
//
//	@Autowired
//	private MsgQueueMapper msgQueueMapper;
//
//	public void insertMsg(MsgPo msgPo) {
//		msgQueueMapper.insert(msgPo);
//	}
//
//
//	public List<MsgPo> loadMsg(List<String> topics, int batchSize) {
//		return msgQueueMapper.select(
//				ImmutableMap.of("topics", topics,
//						"batchSize", batchSize,
//						"locked", 0,
//						"nextRetryTimeLT", new Date()
//				));
//	}
//
//	public boolean lockMsg(MsgPo msg) {
//		return msgQueueMapper.update(
//				ImmutableMap.of("locked", 1, "updateTime", new Date()),
//				ImmutableMap.of("id", msg.getId(),
//						"locked", 0,
//						"updateTime", msg.getUpdateTime(),
//						"nextRetryTime", msg.getNextRetryTime())) > 0;
//	}
//
//	public boolean deleteMsg(long msgId) {
//		return msgQueueMapper.deleteById(msgId) > 0;
//	}
//
//	public boolean retryMsg(MsgPo msgPo) {
//
//		int retried = msgPo.getRetriedTimes();
//		msgPo.setRetriedTimes(retried + 1);
//		Date nextRetryTime;
//		if (retried >= RETRY_GAP.length) {
//			nextRetryTime = UNREACHABLE_TIME;
//		} else {
//			nextRetryTime = new Date(System.currentTimeMillis() + 1000L * RETRY_GAP[retried]);
//		}
//		msgPo.setNextRetryTime(nextRetryTime);
//		msgPo.setUpdateTime(new Date());
//		msgPo.setLocked(0);
//
//		return msgQueueMapper.update(
//				ImmutableMap.of(
//						"retriedTimes", msgPo.getRetriedTimes(),
//						"nextRetryTime", msgPo.getNextRetryTime(),
//						"updateTime", msgPo.getUpdateTime(),
//						"locked", msgPo.getLocked()),
//				ImmutableMap.of("id", msgPo.getId())) > 0;
//	}
//
//	public int fixDeadMsg() {
//		Date now = new Date();
//		return msgQueueMapper.update(
//				ImmutableMap.of("locked", 0, "updateTime", now),
//				ImmutableMap.of("locked", 1, "updateTimeLT", new Date(now.getTime() - MSG_MAX_LOCK_DURATION)));
//	}
//
//	public int cleanAbortMsg() {
//		Date now = new Date();
//		return msgQueueMapper.deleteAborted(UNREACHABLE_TIME, new Date(now.getTime() - ABORT_MSG_MAX_DURATION));
//	}
//}
