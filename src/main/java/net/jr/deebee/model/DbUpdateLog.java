package net.jr.deebee.model;

import java.util.Date;

public class DbUpdateLog {

    private long id;

    private String level;

    private String comment;

    private Date tstamp;

    public static DbUpdateLog createNew(String level, String message){
        DbUpdateLog l = new DbUpdateLog();
        l.setLevel(level);
        l.setComment(message);
        return l;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getTstamp() {
        return tstamp;
    }

    public void setTstamp(Date tstamp) {
        this.tstamp = tstamp;
    }
}
