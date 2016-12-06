package io.mycat.backend.mysql.xa;

/**
 * Created by zhangchao on 2016/10/13.
 */
public enum TxState {
	/** XA INIT STATUS **/
	TX_INITIALIZE_STATE(0),
	/** XA STARTED STATUS **/
	TX_STARTED_STATE(1),
	/** XA ENDED STATUS **/
	TX_ENDED_STATE(2),
	/** XA is prepared **/
	TX_PREPARED_STATE(3),
	/** XA is commited **/
	TX_COMMITED_STATE(4),
	/** XA is rollbacked **/
	TX_ROLLBACKED_STATE(5);
	private int value = 0;

	private TxState(int value) {
		this.value = value;
	}

	public static TxState valueof(int value) {
		switch (value) {
		case 0:
			return TX_INITIALIZE_STATE;
		case 1:
			return TX_STARTED_STATE;
		case 2:
			return TX_ENDED_STATE;
		case 3:
			return TX_PREPARED_STATE;
		case 4:
			return TX_COMMITED_STATE;
		case 5:
			return TX_ROLLBACKED_STATE;
		default:
			return null;
		}
	}

	@Override
	public String toString() {
		return String.valueOf(this.value);
	}
}
