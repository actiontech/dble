package io.mycat.backend.mysql.xa;

import java.io.Serializable;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class CoordinatorLogEntry implements Serializable {
	private static final long serialVersionUID = -919666492191340531L;
	public static final String ID = "id";
	public static final String STATE = "state";
	public static final String PARTICIPANTS = "participants";
	public static final String P_HOST = "host";
	public static final String P_PORT = "port";
	public static final String P_STATE = "p_state";
	public static final String P_EXPIRES = "expires";
	public static final String P_SCHEMA = "schema";
	private final String id;
	private final ParticipantLogEntry[] participants;
	/* session TxState */
	private TxState txState;

	public CoordinatorLogEntry(String coordinatorId, ParticipantLogEntry[] participants, TxState txState) {
		this.id = coordinatorId;
		this.participants = participants;
		this.txState = txState;
	}

	public TxState getTxState() {
		return txState;
	}

	public void setTxState(TxState txState) {
		this.txState = txState;
	}

	public String getId() {
		return id;
	}

	public ParticipantLogEntry[] getParticipants() {
		return participants;
	}
}
