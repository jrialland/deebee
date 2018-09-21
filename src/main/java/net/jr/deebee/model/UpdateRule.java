package net.jr.deebee.model;

import com.github.zafarkhaja.semver.Version;
import org.jdeferred.Deferred;

public class UpdateRule {

    private UpdatePlan plan;

    private String comment, reference;

    private Version fromVersion, targetVersion;

    private Deferred<UpdateRule, ?, ?> action;

    public UpdateRule(UpdatePlan plan) {
        this.plan = plan;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Version getFromVersion() {
        return fromVersion;
    }

    public void setFromVersion(Version fromVersion) {
        this.fromVersion = fromVersion;
    }

    public Version getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(Version targetVersion) {
        this.targetVersion = targetVersion;
    }

    public Deferred<UpdateRule, ?, ?> getAction() {
        return action;
    }

    public void setAction(Deferred<UpdateRule, ?, ?> action) {
        this.action = action;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public String getReference() {
        return reference;
    }

    public UpdatePlan getPlan() {
        return plan;
    }
}
