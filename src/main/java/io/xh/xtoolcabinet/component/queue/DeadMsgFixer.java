package io.xh.xtoolcabinet.component.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeadMsgFixer {

	private static final Logger logger = LoggerFactory.getLogger(DeadMsgFixer.class);

	private ScheduledExecutorService ses;

	private MsgQueueService msgQueueService;

	private AtomicBoolean started = new AtomicBoolean(false);

	private static DeadMsgFixer deadMsgFixer;

	public static DeadMsgFixer create(MsgQueueService msgQueueService) {
		if (deadMsgFixer == null) {
			synchronized (DeadMsgFixer.class) {
				if (deadMsgFixer == null) {
					DeadMsgFixer newInstance = new DeadMsgFixer(msgQueueService);
					deadMsgFixer = newInstance;
				}
			}
		}
		return deadMsgFixer;
	}

	private DeadMsgFixer(MsgQueueService msgQueueService) {
		this.msgQueueService = msgQueueService;
	}

	public void start() {
		if (started.compareAndSet(false, true)) {

			logger.info("DeadMsgFixer start ...");

			ses = new ScheduledThreadPoolExecutor(1);
			ses.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					fixDeadMsg();
				}
			}, new Random().nextInt(60), 60 * 30, TimeUnit.SECONDS);


			ses.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					cleanAbortMsg();
				}
			}, new Random().nextInt(60 * 60), 60 * 60 * 24, TimeUnit.SECONDS);
		}
	}

	private void fixDeadMsg() {
		logger.info("fix dead msg start");
		logger.info("fix dead msg end, num: {}", msgQueueService.fixDeadMsg());
	}

	private void cleanAbortMsg() {
		logger.info("clean abort msg start");
		logger.info("clean abort msg end, num: {}", msgQueueService.cleanAbortMsg());
	}

	// 动态关闭可能会有问题，因为DeadMsgFixer是单例，由所有Consumer共享的
	// 不应该是某一个Consumer关闭就关闭，而是最后一个Consumer关闭才关闭
	public void stop() {
		if (ses != null) {
			logger.info("DeadMsgFixer stop ...");

			ses.shutdownNow();
			ses = null;
		}
	}
}
