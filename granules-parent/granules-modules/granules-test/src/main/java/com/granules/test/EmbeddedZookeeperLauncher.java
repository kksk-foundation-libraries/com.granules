package com.granules.test;

import org.apache.curator.test.TestingServer;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class EmbeddedZookeeperLauncher implements TestRule {
	private String connectString;

	public EmbeddedZookeeperLauncher() {
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				TestingServer testingServer = new TestingServer();
				testingServer.start();
				connectString = testingServer.getConnectString();
				base.evaluate();
				testingServer.stop();
				testingServer.close();
			}
		};
	}

	public String connectString() {
		return connectString;
	}
}
