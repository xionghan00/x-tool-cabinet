package io.xh.xtoolcabinet.tools;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by HanXiong on 2017/7/5.
 */
public class RetryWorkingCenter<T> {

    private static final Logger logger = LoggerFactory.getLogger(RetryWorkingCenter.class);

    private RetryWorker retrier;

    private String retryDelayLevelConfig = "5s 30s 1m 3m 10m 30m 1h";

    private AtomicBoolean started = new AtomicBoolean(false);

    private String identity;

    private Work<T> work;

    public RetryWorkingCenter(String identity, Work<T> work) {
        this.identity = identity;
        this.work = work;
    }

    public void doWork(T t) {
        retrier.doWork(t);
    }

    public void setRetryDelayLevelConfig(String s) {
        this.retryDelayLevelConfig = s;
    }

    public void start() {

        if (started.compareAndSet(false, true)) {

            String path = System.getProperty("user.home") + File.separator + ".retryWorkingCenter" + File.separator + identity;

            new File(path).mkdirs();

            retrier = new RetryWorker(path, parseDelayLevel(retryDelayLevelConfig));

            retrier.start();
        }
    }

    public void stop() {
        if (retrier != null) {
            retrier.stop();
        }
    }

    private List<Long> parseDelayLevel(String s) {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        try {
            String[] levelArray = retryDelayLevelConfig.split(" ");
            List<Long> result = new ArrayList<>(levelArray.length);

            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);
                long num = Long.parseLong(value.substring(0, value.length() - 1));

                result.add(tu * num);
            }

            return result;
        } catch (Exception e) {
            logger.error("parseDelayLevel fail", e);
            return null;
        }
    }

    public interface Work<T> {
        boolean doWork(T t);

        void onRetryTimesOver(T t);
    }





    private class RetryWorker {

        private String dbPath;

        private DB db;

        private BTreeMap<String, WorkExtInfo> dataMap;

        private ServiceThread notifyThread;

        private List<Long> retryDelayLevel;

        private final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        RetryWorker(String dbPath, List<Long> retryDelayLevel) {
            this.dbPath = dbPath;
            this.db = DBMaker.fileDB(new File(this.dbPath, "retryWorker.db"))
                    .closeOnJvmShutdown()
                    .encryptionEnable("retryWorker")
                    .make();
            this.dataMap = db.treeMap("retryWorker");
            this.retryDelayLevel = retryDelayLevel;

            this.notifyThread = new ServiceThread() {

                private final int DEFAULT_BATCH_SIZE = 5;
                private final List<WorkExtInfo<T>> recycleList = new ArrayList<WorkExtInfo<T>>(DEFAULT_BATCH_SIZE);

                public void run() {

                    logger.info(this.getServiceName() + " service started");

                    while (!this.isStopped()) {
                        try {
                            fetchTop();

                            if (recycleList.size() > 0) {
                                for (WorkExtInfo<T> w : recycleList) {
                                    retry(w);
                                }
                                db.commit();
                            }

                        } catch (Exception e) {
                            logger.error(this.getServiceName() + " Service Run Method exception", e);
                        }
                    }

                    logger.info(this.getServiceName() + " service end");
                }

                private void fetchTop() {
                    recycleList.clear();

                    long now = System.currentTimeMillis();

                    long forWaitTime = -1;

                    Iterator<Map.Entry<String, WorkExtInfo>> itr = dataMap.entrySet().iterator();
                    for (; itr.hasNext() && recycleList.size() < DEFAULT_BATCH_SIZE; ) {
                        WorkExtInfo n = itr.next().getValue();

                        if (now >= n.nextRetryTime) {
                            recycleList.add(n);
                        } else {
                            forWaitTime = n.nextRetryTime - now;
                            break;
                        }
                    }

                    if (recycleList.size() == 0) {
                        if (forWaitTime != -1) {
                            this.waitForRunning(forWaitTime);
                        } else {
                            this.waitForRunning(3 * 1000);
                        }
                    }
                }

                public String getServiceName() {
                    return "RetryWorker";
                }
            };
        }

        void doWork(T w) {
            WorkExtInfo<T> WorkExtInfo = new WorkExtInfo(System.currentTimeMillis(), 0, w);
            dataMap.put(WorkExtInfo.toSortableKey(), WorkExtInfo);
            db.commit();

            notifyThread.wakeup();
        }


        private void retry(WorkExtInfo<T> w) {

            String forRemove = w.toSortableKey();
            if (!work.doWork(w.work)) {
                WorkExtInfo inner = updateRetryContext(w);
                if (inner != null) {
                    dataMap.put(w.toSortableKey(), w);

                    if (logger.isInfoEnabled()) {
                        logger.info("content: {}, will retry at: {}", w.work, sdf.format(new Date(w.nextRetryTime)));
                    }
                }
            }

            RetryWorker.this.dataMap.remove(forRemove);
        }


        private WorkExtInfo updateRetryContext(WorkExtInfo<T> inner) {
            if (inner.retriedTimes >= retryDelayLevel.size()) {
                work.onRetryTimesOver(inner.work);
                return null;
            }

            inner.nextRetryTime = System.currentTimeMillis() + retryDelayLevel.get(inner.retriedTimes);
            inner.retriedTimes++;
            return inner;
        }

        void start() {
            notifyThread.start();
        }

        void stop() {
            notifyThread.stop();
        }
    }

    private static class WorkExtInfo<T> implements Serializable {
        public int retriedTimes;

        public long nextRetryTime;

        public T work;

        public WorkExtInfo(long nextRetryTime, int retriedTimes, T work) {
            this.nextRetryTime = nextRetryTime;
            this.retriedTimes = retriedTimes;
            this.work = work;
        }

        public String toSortableKey() {
            return String.valueOf(nextRetryTime) + work.toString();
        }
    }
}
