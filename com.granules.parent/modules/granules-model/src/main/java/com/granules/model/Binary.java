package com.granules.model;

import java.io.Serializable;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class Binary implements Serializable {
	/** serialVersionUID */
	private static final long serialVersionUID = 1L;
	private byte[] data;

	private Binary() {
	}

	private Binary set(byte[] data) {
		this.data = data;
		return this;
	}

	public static Binary of(ColferObject object) {
		return new Binary().set(object.marshal());
	}

	public static Binary of(byte[] data) {
		return new Binary().set(data);
	}

	public <T extends ColferObject> T mapTo(T target) {
		return target.unmarshal(data);
	}
}
