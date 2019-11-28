package com.granules.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class ColferObject {
	public abstract byte[] marshal(OutputStream out, byte[] buf) throws IOException;

	public abstract int unmarshal(byte[] buf, int offset);

	public final byte[] marshal() {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			return marshal(out, null);
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public final <T extends ColferObject> T unmarshal(byte[] buf) {
		unmarshal(buf, 0);
		return (T) this;
	}
}
