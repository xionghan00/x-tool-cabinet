package io.xh.xtoolcabinet.component.queue;

import java.util.Date;
import java.util.List;

interface MsgQueueService {


	Date UNREACHABLE_TIME = new Date(253370736000000L);

	// 30 min
	long MSG_MAX_LOCK_DURATION = 1000L * 60 * 30;

	// 2 day
	long ABORT_MSG_MAX_DURATION = 1000L * 60 * 60 * 24 * 2;

	int[] RETRY_GAP = new int[]{
			10, 30, 60, 300, 1800, 3600
	};

	void insertMsg(MsgPo msgPo);

	List<MsgPo> loadMsg(List<String> topics, int batchSize);

	boolean lockMsg(MsgPo msg);

	boolean deleteMsg(long msgId);

	boolean retryMsg(MsgPo msgPo);

	int fixDeadMsg();

	int cleanAbortMsg();
}
