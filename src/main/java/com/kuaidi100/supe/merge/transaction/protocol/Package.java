package com.kuaidi100.supe.merge.transaction.protocol;

public class Package {
    private Header header;
    private Body body;

    public Package(Header header, Body body) {
        this.header = header;
        this.body = body;
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public Body getBody() {
        return body;
    }

    public void setBody(Body body) {
        this.body = body;
    }
}
