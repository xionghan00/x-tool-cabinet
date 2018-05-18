package io.xh.xtoolcabinet.component.queue;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 用乐观锁的方式实现的并行消费，这种方式会产生很多失败的tryLock操作
 * 实现上比较简单，但是总体性能比较差。不过以n-des当前的数据量足够了。
 */
public class MsgConsumer {

	private static final Logger logger = LoggerFactory.getLogger(MsgConsumer.class);

	private Map<String, MsgExecutor> msgExecutors = new HashMap<>();

	private MsgQueueService msgQueueService;

	private MsgLoader msgLoader;

	private DeadMsgFixer deadMsgFixer;

	public MsgConsumer(MsgQueueService msgQueueService) {
		this.msgQueueService = msgQueueService;
		this.deadMsgFixer = DeadMsgFixer.create(this.msgQueueService);
	}

	public void subscribe(String topic, int concurrency, MsgListener listener) {
		msgExecutors.put(topic, new MsgExecutor(topic, concurrency, listener));
	}

	public void start() {
		msgLoader = new MsgLoader();
		new Thread(msgLoader).start();
		this.deadMsgFixer.start();
	}

	public void stop() {
		msgLoader.stop();
		this.deadMsgFixer.stop();
		for (MsgExecutor es : msgExecutors.values()) {
			es.shutdown();
		}
	}

	private class MsgLoader implements Runnable {

		private volatile boolean loadThreadRunning = true;

		private LinkedBlockingQueue<LoadRequest> loadRequestQueue = new LinkedBlockingQueue<>();

		private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
				new Thread(r, "LoadRequestScheduledThread")
		);

		private volatile Thread thisThread;

		private int batchSize = 8;


		@Override
		public void run() {
			thisThread = Thread.currentThread();

			final List<String> loadTopics = Lists.newArrayList(msgExecutors.keySet());

			for (String topic : loadTopics) {
				loadImmediately(new LoadRequest(topic));
			}

			logger.info("MsgLoader start...");

			while (loadThreadRunning && !Thread.currentThread().isInterrupted()) {
				try {
					LoadRequest request;
					try {
						request = loadRequestQueue.take();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						continue;
					}

					processLoadRequest(request);

				} catch (Exception e) {
					// to ensure the thread loop not quit when exception occurs.
					// sleep a while to void fast retry in bad situation
					logger.error("unknown exception", e);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						continue;
					}
				}
			}

			logger.info("MsgLoader quit...");
		}

		private void processLoadRequest(LoadRequest request) {
			final String topic = request.topic;
			int executeNum = 0;

			try {
				List<MsgPo> msgPos = msgQueueService.loadMsg(ImmutableList.of(topic), batchSize);

				if (!msgPos.isEmpty()) {
					final MsgExecutor msgExecutor = msgExecutors.get(topic);
					for (MsgPo po : msgPos) {
						if (msgExecutor.tryExecute(po)) {
							executeNum++;
						}
					}
				}
			} catch (Exception e) {
				logger.error("unknown execute exception", e);
			}

			if (executeNum != 0) {
				request.loadDelayUnit = 0;
				loadImmediately(request);
			} else {
				increaseLoadDelayUnit(request);
				loadWithDelay(request, request.loadDelayUnit * 500);
			}
		}

		private void loadImmediately(LoadRequest r) {
			try {
				loadRequestQueue.put(r);
			} catch (InterruptedException e) {
				logger.error("loadImmediately InterruptedException", e);
			}
		}

		private void loadWithDelay(LoadRequest r, long delay) {
			scheduledExecutorService.schedule(() -> loadImmediately(r), delay, TimeUnit.MILLISECONDS);
		}

		private void increaseLoadDelayUnit(LoadRequest r) {
			r.loadDelayUnit++;
			if (r.loadDelayUnit > 10) {
				r.loadDelayUnit = 10;
			}
		}

		public void stop() {
			loadThreadRunning = false;
			thisThread.interrupt();
			scheduledExecutorService.shutdown();
		}
	}

	private class MsgExecutor {

		private Semaphore semaphore;
		private ExecutorService es;
		private MsgListener msgListener;

		public MsgExecutor(String topic, int concurrency, MsgListener listener) {
			this.semaphore = new Semaphore(concurrency);
			this.es = createExecutorService(topic, concurrency);
			this.msgListener = listener;
		}

		public boolean tryExecute(MsgPo msg) {
			if (!semaphore.tryAcquire()) {
				return false;
			}

			try {
				// lock msg fail, return resource
				if (!msgQueueService.lockMsg(msg)) {
					if (logger.isDebugEnabled()) {
						logger.debug("lock fail. msg: {}", msg);
					}
					semaphore.release();
					return true;
				}
				if (logger.isDebugEnabled()) {
					logger.debug("lock suc. msg: {}", msg);
				}
				es.submit(() -> {
					ConsumeResultAction resultAction;
					try {
						try {
							resultAction = msgListener.consume(new ConsumeContext(msg));

							if (resultAction == null) {
								resultAction = ConsumeResultAction.SUCCESS;
							}

						} catch (Exception e) {
							logger.error("consume exception", e);
							resultAction = ConsumeResultAction.RETRY;
						}

						switch (resultAction) {
							case SUCCESS:
								msgQueueService.deleteMsg(msg.getId());
								if (logger.isDebugEnabled()) {
									logger.debug("delete msg: {}", msg);
								}
								break;
							case RETRY:
								msgQueueService.retryMsg(msg);
								if (logger.isDebugEnabled()) {
									logger.debug("retry msg: {}", msg);
								}
								break;
						}

					} catch (Exception e) {
						logger.error("unknown exception", e);
					} finally {
						semaphore.release();
					}
				});

			} catch (Throwable t) {
				// protection for semaphore leakage
				semaphore.release();
				throw t;
			}

			return true;
		}

		public void shutdown() {
			this.es.shutdown();
		}
	}

	private class LoadRequest {
		String topic;
		int loadDelayUnit;

		public LoadRequest(String topic) {
			this.topic = topic;
			this.loadDelayUnit = 0;
		}
	}

	private ThreadPoolExecutor createExecutorService(String topic, int concurrency) {
		return new ThreadPoolExecutor(concurrency, concurrency, 30, TimeUnit.SECONDS,
				new LinkedBlockingDeque<>(),
				new ThreadFactoryBuilder().setNameFormat("msg-consumer-" + topic + "-%d").build(),
				new ThreadPoolExecutor.AbortPolicy());
	}
}
