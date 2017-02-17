package io.mycat.plan.common.ptr;

public class GenericPtr<T> {
	private T value;

	public GenericPtr(T initValue) {
		this.value = initValue;
	}

	public T get() {
		return value;
	}

	public void set(T value) {
		this.value = value;
	}

}
