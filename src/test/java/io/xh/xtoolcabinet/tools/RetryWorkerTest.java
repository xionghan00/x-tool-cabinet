package io.xh.xtoolcabinet.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class RetryWorkerTest {

	static final Logger logger = LoggerFactory.getLogger(RetryWorkerTest.class);

	public static void main(String[] args) throws InterruptedException {

		RetryWorkingCenter<String> retryWorkingCenter = new RetryWorkingCenter<String>("123", new RetryWorkingCenter.Work<String>() {
			public boolean doWork(String s) {
				logger.info("do work: " + s);

				if (s.startsWith("a")) {
					return true;
				}

				return false;
			}

			public void onRetryTimesOver(String s) {
				logger.error("abort work: " + s);
			}
		});

		retryWorkingCenter.setRetryDelayLevelConfig("1s 5s 10s");

		retryWorkingCenter.start();

		Scanner scanner = new Scanner(System.in);
		String s = null;
		while ((s = scanner.next()) != null) {
			if ("stop".equals(s)) {
				break;
			}
			retryWorkingCenter.doWork(s);
		}



		Thread.sleep(1000L * 50);
		retryWorkingCenter.stop();
	}


}
