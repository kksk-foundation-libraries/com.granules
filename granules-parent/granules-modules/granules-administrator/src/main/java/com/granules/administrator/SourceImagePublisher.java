package com.granules.administrator;

import java.io.StringReader;
import java.util.concurrent.Future;

public interface SourceImagePublisher {
	Future<Void> publish(Integer id, String fqcn, StringReader sourceReader);
}
