package com.granules.source.subscriber;

import java.io.Closeable;

public interface SourceImageSubscriber {
	Closeable subscribe();
}
