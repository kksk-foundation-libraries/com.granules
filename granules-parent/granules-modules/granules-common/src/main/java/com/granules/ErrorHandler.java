package com.granules;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ErrorHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);
	private final Consumer<Exception> errorHandler;
	private final Consumer<Exception> warnHandler;

	protected ErrorHandler(Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		this.errorHandler = errorHandler;
		this.warnHandler = warnHandler;
	}

	protected void onWarnning(Exception e) {
		LOG.warn("warn raised.", e);
		if (warnHandler != null) {
			warnHandler.accept(e);
		}
	}

	protected void onError(Exception e) {
		LOG.error("error raised.", e);
		if (errorHandler != null) {
			errorHandler.accept(e);
		}
	}
}
