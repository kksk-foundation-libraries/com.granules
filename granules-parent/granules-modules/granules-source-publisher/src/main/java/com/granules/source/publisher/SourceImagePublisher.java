package com.granules.source.publisher;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface SourceImagePublisher {
	default Future<Void> publish(Integer id, String fqcn, String source) {
		ByteArrayInputStream in = new ByteArrayInputStream(source.getBytes());
		return publish(id, fqcn, in);
	}

	default Future<Void> publish(Integer id, String fqcn, File sourceFile) {
		try {
			return publish(id, fqcn, new FileInputStream(sourceFile));
		} catch (FileNotFoundException e) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			future.completeExceptionally(e);
			return future;
		}
	}

	Future<Void> publish(Integer id, String fqcn, InputStream sourceInputStream);
}
