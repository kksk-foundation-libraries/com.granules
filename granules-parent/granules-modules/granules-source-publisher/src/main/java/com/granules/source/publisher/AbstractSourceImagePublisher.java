package com.granules.source.publisher;

import java.util.function.Consumer;

import com.granules.ErrorHandler;

public abstract class AbstractSourceImagePublisher extends ErrorHandler implements SourceImagePublisher {
	protected AbstractSourceImagePublisher(Consumer<Exception> errorHandler, Consumer<Exception> warnHandler) {
		super(errorHandler, warnHandler);
	}
}
