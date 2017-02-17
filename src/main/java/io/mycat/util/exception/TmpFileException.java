package io.mycat.util.exception;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Properties;


public class TmpFileException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	private static final Properties MESSAGES = new Properties();
	static {
		try {
			InputStream in = TmpFileException.class.getResourceAsStream("/io/mycat/util/exception/res/_messages_en.prop");
			if (in != null) {
				MESSAGES.load(in);
			}
			String language = Locale.getDefault().getLanguage();
			if (!"en".equals(language)) {

			}
		} catch (IOException e) {
		}
	}

	private TmpFileException(String message) {
		super(message, null);
	}

	private TmpFileException(String message, Throwable cause) {
		super(message, cause);
	}

	public static TmpFileException get(int errorCode, String... params) {
		String message = translate(String.valueOf(errorCode), params);
		return new TmpFileException(message);
	}

	public static TmpFileException get(int errorCode, Throwable cause, String... params) {
		String message = translate(String.valueOf(errorCode), params);
		return new TmpFileException(message, cause);
	}

	private static String translate(String key, String... params) {
		String message = null;
		if (MESSAGES != null) {
			// Tomcat sets final static fields to null sometimes
			message = MESSAGES.getProperty(key);
		}
		if (message == null) {
			message = "(Message " + key + " not found)";
		}
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				String s = params[i];
				if (s != null && s.length() > 0) {
					params[i] = quoteIdentifier(s);
				}
			}
			message = MessageFormat.format(message, (Object[]) params);
		}
		return message;
	}

	private static String quoteIdentifier(String s) {
		int length = s.length();
		StringBuilder buff = new StringBuilder(length + 2);
		buff.append('\"');
		for (int i = 0; i < length; i++) {
			char c = s.charAt(i);
			if (c == '"') {
				buff.append(c);
			}
			buff.append(c);
		}
		return buff.append('\"').toString();
	}
	/**
	 * Create a ares exception for a specific error code.
	 * 
	 * @param errorCode
	 *            the error code
	 * @param p1
	 *            the first parameter of the message
	 * @return the exception
	 */
	public static TmpFileException get(int errorCode, String p1) {
		return get(errorCode, new String[] { p1 });
	}

	/**
	 * Convert an IO exception to a database exception.
	 * 
	 * @param e
	 *            the root cause
	 * @param message
	 *            the message or null
	 * @return the database exception object
	 */
	public static TmpFileException convertIOException(int errorCode, IOException e, String message) {
		if (message == null) {
			Throwable t = e.getCause();
			if (t instanceof TmpFileException) {
				return (TmpFileException) t;
			}
			return get(errorCode, e.toString());
		}
		return get(errorCode * 10, e.toString(), message);
	}

	/**
	 * Gets a exception meaning this value is invalid.
	 * 
	 * @param param
	 *            the name of the parameter
	 * @param value
	 *            the value passed
	 * @return the IllegalArgumentException object
	 */
	public static TmpFileException getInvalidValueException(int errorCode, String param, Object value) {
		return get(errorCode, value == null ? "null" : value.toString(), param);
	}
}
