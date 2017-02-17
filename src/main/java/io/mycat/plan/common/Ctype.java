package io.mycat.plan.common;

public class Ctype {
	public static int _MY_U = 01; /* Upper case */
	public static int _MY_L = 02; /* Lower case */
	public static int _MY_NMR = 04; /* Numeral (digit) */
	public static int _MY_SPC = 010; /* Spacing character */
	public static int _MY_PNT = 020; /* Punctuation */
	public static int _MY_CTR = 040; /* Control character */
	public static int _MY_B = 0100; /* Blank */
	public static int _MY_X = 0200; /* heXadecimal digit */

	private static byte ctype_latin1[] = { 0, 32, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 32, 32, 32, 32,
			32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
			16, 16, 16, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132, (byte) 132,
			(byte) 132, (byte) 132, 16, 16, 16, 16, 16, 16, 16, (byte) 129, (byte) 129, (byte) 129, (byte) 129,
			(byte) 129, (byte) 129, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 16, 16, 16, 16, 16,
			(byte) 130, (byte) 130, (byte) 130, (byte) 130, (byte) 130, (byte) 130, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			2, 2, 2, 2, 2, 2, 2, 2, 16, 16, 16, 16, 32, 16, 0, 16, 2, 16, 16, 16, 16, 16, 16, 1, 16, 1, 0, 1, 0, 0, 16,
			16, 16, 16, 16, 16, 16, 16, 16, 2, 16, 2, 0, 2, 1, 72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
			16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
			2, 2, 2, 2, 2, 2, 2, 16, 2, 2, 2, 2, 2, 2, 2, 2 };

	/**
	 * 目前仅作latin的
	 * 
	 * @param charset
	 * @param c
	 * @return
	 */
	public static boolean isDigit( // String charset,
			char c) {
		int index = (int) c + 1;
		return (ctype_latin1[index] & _MY_NMR) != 0;
	}

	public static boolean my_isalpha(char c) {
		int index = (int) c + 1;
		return (ctype_latin1[index] & (_MY_U | _MY_L)) != 0;
	}

	public static boolean spaceChar(char c) {
		int index = (int) c + 1;
		return (ctype_latin1[index] & _MY_SPC) != 0;
	}

	public static boolean isPunct(char c) {
		int index = (int) c + 1;
		return (ctype_latin1[index] & _MY_PNT) != 0;
	}

	/**
	 * compare cs[start~count]==cs2[start~count] see
	 * ctype-simple.c[my_strnncoll_simple]
	 * 
	 * @param cs
	 * @param start
	 * @param count
	 * @param cs2
	 * @param start
	 * @param count
	 */
	public static int my_strnncoll(char[] css, int sbegin, int slen, char[] cst, int tbegin, int tlen) {
		return new String(css, sbegin, slen).compareTo(new String(cst, tbegin, tlen));
	}

}
