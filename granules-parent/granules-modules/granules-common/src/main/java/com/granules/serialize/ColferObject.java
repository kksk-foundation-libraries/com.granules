package com.granules.serialize;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * usage:
 * <pre>
 * Write:
 * 	{Your Data Class} data = ...;
 *	Kryo kryo = new Kryo();
 *	kryo.register({Your Data Class}.class, {Your Data Class ID);
 *	Output output = new Output(outputStream);
 *	kryo.writeClassAndObject(output, data);
 *	output.close();
 * Read:
 *	Kryo kryo = new Kryo();
 *	kryo.register({Your Data Class}.class, {Your Data Class ID);
 *	Input input = new Input(inputStream);
 *	{Your Data Class} data = (Your Data Class) kryo.readClassAndObject(input);
 * </pre>
 */
public interface ColferObject extends KryoSerializable {
	byte[] marshal(OutputStream out, byte[] buf) throws IOException;

	int unmarshal(byte[] buf, int offset);

	default byte[] marshal() {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			marshal(out, null);
			return out.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	default <T extends ColferObject> T unmarshal(byte[] buf) {
		unmarshal(buf, 0);
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	default <T extends ColferObject> T cast() {
		return (T) this;
	}

	@Override
	default void read(Kryo kryo, Input input) {
		try {
			int size = input.available();
			unmarshal(input.readBytes(size));
		} catch (IOException ignore) {
			// noop
		}
	}

	@Override
	default void write(Kryo kryo, Output output) {
		output.write(marshal());
	}
}
