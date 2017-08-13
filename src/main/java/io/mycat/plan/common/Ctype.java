package io.mycat.plan.common;

public class Ctype {
	private static final int _MY_U = 1; /* Upper case */
	private static final  int _MY_L = 2; /* Lower case */
	private static final  int _MY_NMR = 4; /* Numeral (digit) */
	private static final  int _MY_SPC = 8; /* Spacing character */
	private static final  int _MY_PNT = 16; /* Punctuation */
	public static final  int _MY_CTR = 32; /* Control character */
	public static final  int _MY_B = 64; /* Blank */
	public static final  int _MY_X = 128; /* heXadecimal digit */

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


	public static int my_strnncoll(char[] css, int sbegin, int slen, char[] cst, int tbegin, int tlen) {
		return new String(css, sbegin, slen).compareTo(new String(cst, tbegin, tlen));
	}

}
