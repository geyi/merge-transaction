package com.kuaidi100.supe.merge.transaction.protocol;

import java.io.Serializable;

public class Header implements Serializable {
    private long requestId;

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }
}
