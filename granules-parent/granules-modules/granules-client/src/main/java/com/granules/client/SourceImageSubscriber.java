package com.granules.client;

import java.io.Closeable;

public interface SourceImageSubscriber {
	Closeable subscribe();
}
