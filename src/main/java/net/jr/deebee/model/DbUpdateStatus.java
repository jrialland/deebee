package net.jr.deebee.model;

import java.util.Date;

public class DbUpdateStatus {

    private long id;

    private String fromVersion;

    private String toVersion;

    private Date since;

    private boolean current;

    private String user;

    private String reference;

    private String comment;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getFromVersion() {
        return fromVersion;
    }

    public void setFromVersion(String fromVersion) {
        this.fromVersion = fromVersion;
    }

    public String getToVersion() {
        return toVersion;
    }

    public void setToVersion(String toVersion) {
        this.toVersion = toVersion;
    }

    public Date getSince() {
        return since;
    }

    public void setSince(Date since) {
        this.since = since;
    }

    public boolean isCurrent() {
        return current;
    }

    public void setCurrent(boolean current) {
        this.current = current;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getReference() {
        return reference;
    }
}
