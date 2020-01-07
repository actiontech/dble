/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

/**
 * Created by zhangchao on 2016/10/13.
 */
public enum TxState {
    /**
     * XA INIT STATUS
     **/
    TX_INITIALIZE_STATE(0),
    /**
     * XA STARTED STATUS
     **/
    TX_STARTED_STATE(1),
    /**
     * XA ENDED STATUS
     **/
    TX_ENDED_STATE(2),
    /**
     * XA is prepared
     **/
    TX_PREPARED_STATE(3),
    /**
     * XA is prepare unconnect
     **/
    TX_PREPARE_UNCONNECT_STATE(4),
    /**
     * XA is commit failed,must be commited again
     **/
    TX_COMMIT_FAILED_STATE(5),
    /**
     * XA is rollbacked
     **/
    TX_ROLLBACK_FAILED_STATE(6),
    /**
     * XA conn is quit
     **/
    TX_CONN_QUIT(7),
    /**
     * XA is committed, just for log
     */
    TX_COMMITTED_STATE(8),
    /**
     * XA is rollbacked, just for log
     */
    TX_ROLLBACKED_STATE(9),
    /**
     * XA is committing, just for log
     */
    TX_COMMITTING_STATE(10),
    /**
     * XA is rollbacking, rollback after prepared ,just for log
     */
    TX_ROLLBACKING_STATE(11),
    /**
     * XA is rollbacking, rollback after prepared ,just for log
     */
    TX_PREPARING_STATE(12);
    private int value = 0;

    TxState(int value) {
        this.value = value;
    }

    public static TxState valueOf(int value) {
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
                return TX_PREPARE_UNCONNECT_STATE;
            case 5:
                return TX_COMMIT_FAILED_STATE;
            case 6:
                return TX_ROLLBACK_FAILED_STATE;
            case 7:
                return TX_CONN_QUIT;
            case 8:
                return TX_COMMITTED_STATE;
            case 9:
                return TX_ROLLBACKED_STATE;
            case 10:
                return TX_COMMITTING_STATE;
            case 11:
                return TX_ROLLBACKING_STATE;
            case 12:
                return TX_PREPARING_STATE;
            default:
                return null;
        }
    }

    public String getState() {
        switch (value) {
            case 0:
                return "TX_INITIALIZE_STATE";
            case 1:
                return "TX_STARTED_STATE";
            case 2:
                return "TX_ENDED_STATE";
            case 3:
                return "TX_PREPARED_STATE";
            case 4:
                return "TX_PREPARE_UNCONNECT_STATE";
            case 5:
                return "TX_COMMIT_FAILED_STATE";
            case 6:
                return "TX_ROLLBACK_FAILED_STATE";
            case 7:
                return "TX_CONN_QUIT";
            case 8:
                return "TX_COMMITTED_STATE";
            case 9:
                return "TX_ROLLBACKED_STATE";
            case 10:
                return "TX_COMMITTING_STATE";
            case 11:
                return "TX_ROLLBACKING_STATE";
            case 12:
                return "TX_PREPARING_STATE";
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
