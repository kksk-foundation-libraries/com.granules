package com.granules.administrator;

import java.io.Reader;
import java.util.concurrent.Future;

public interface SourceImagePublisher {
	Future<Void> publish(Integer id, String fqcn, Reader sourceReader);
}
