package com.granules.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SimpleLogging implements TestRule {
	@Override
	public Statement apply(Statement base, Description description) {
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				base.evaluate();
			}
		};
	}

	public static SimpleLogging getInstance(Class<?> clazz) {
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy/MM/dd HH:mm:ss.SSS");
		System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
		System.setProperty("org.slf4j.simpleLogger.log." + clazz.getPackage().getName(), "DEBUG");
		return new SimpleLogging();
	}

	public static SimpleLogging getInstance(String... groups) {
		System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
		System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy/MM/dd HH:mm:ss.SSS");
		System.setProperty("org.slf4j.simpleLogger.showThreadName", "true");
		for (String group : groups) {
			System.setProperty("org.slf4j.simpleLogger.log." + group, "DEBUG");
		}
		return new SimpleLogging();
	}
}
