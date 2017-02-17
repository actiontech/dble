package io.mycat.plan.common.exception;

public class TempTableException extends RuntimeException {

	private static final long serialVersionUID = 2869994979718401423L;

	public TempTableException(String message, Throwable cause) {
		super(message, cause);
	}

	public TempTableException(String message) {
		super(message);
	}

	public TempTableException(Throwable cause) {
		super(cause);
	}
}