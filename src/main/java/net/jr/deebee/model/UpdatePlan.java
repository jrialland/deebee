package net.jr.deebee.model;

import net.jr.deebee.UpdatePlanConfig;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class UpdatePlan {

    private Set<UpdateRule> rules = new HashSet<>();

    private Properties properties = new Properties();

    private UpdatePlanConfig config;

    public Set<UpdateRule> getRules() {
        return rules;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public UpdatePlanConfig getConfig() {
        return config;
    }

    public void setConfig(UpdatePlanConfig config) {
        this.config = config;
    }
}
