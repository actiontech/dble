/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

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

    public CoordinatorLogEntry getDeepCopy() {
        ParticipantLogEntry[] newParticipants = new ParticipantLogEntry[participants.length];
        for (int i = 0; i < participants.length; i++) {
            if (participants[i] == null) {
                return null;
            }
            newParticipants[i] = new ParticipantLogEntry(participants[i].getCoordinatorId(), participants[i].getHost(),
                    participants[i].getPort(), participants[i].getExpires(), participants[i].getSchema(),
                    participants[i].getTxState());
        }
        return new CoordinatorLogEntry(id, newParticipants, txState);
    }
}
