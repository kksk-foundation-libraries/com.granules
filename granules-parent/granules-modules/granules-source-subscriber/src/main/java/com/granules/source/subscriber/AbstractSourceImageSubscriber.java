package com.granules.source.subscriber;

import java.util.function.Consumer;

import com.granules.ErrorHandler;

public abstract class AbstractSourceImageSubscriber extends ErrorHandler implements SourceImageSubscriber {
	protected AbstractSourceImageSubscriber(Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		super(errorHandler, warnHandler);
	}
}
