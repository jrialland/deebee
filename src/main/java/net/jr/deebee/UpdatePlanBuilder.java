package net.jr.deebee;

import com.github.zafarkhaja.semver.Version;
import net.jr.deebee.dao.Dao;
import net.jr.deebee.dao.H2Dao;
import net.jr.deebee.model.DbUpdateStatus;
import net.jr.deebee.model.UpdatePlan;
import net.jr.deebee.model.UpdateRule;
import net.jr.deebee.util.sql.SqlScriptRunner;
import org.jdeferred.Deferred;
import org.jdeferred.DoneCallback;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UpdatePlanBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePlanBuilder.class);

    public static final String INITIAL_VERSION = "0.0.0";

    public class UpdateRuleBuilder {

        private DbUpdateStatus dbUpdateStatus = new DbUpdateStatus();

        private Deferred<UpdateRule, ?, ?> action;

        public UpdateRuleBuilder fromVersion(String version) {
            dbUpdateStatus.setFromVersion(version);
            return this;
        }

        public UpdateRuleBuilder toVersion(String version) {
            dbUpdateStatus.setToVersion(version);
            return this;
        }

        public UpdateRuleBuilder withComment(String comment) {
            dbUpdateStatus.setComment(comment);
            return this;
        }

        public UpdateRuleBuilder withReference(String reference) {
            dbUpdateStatus.setReference(reference);
            return this;
        }

        public Promise<UpdateRule, ?, ?> action(DoneCallback<UpdateRule> callback) {
            this.action = new DeferredObject<>();
            return this.action.then(callback);
        }

    }

    private Set<UpdateRuleBuilder> updateRules = new HashSet<>();

    private UpdatePlanConfig config;

    public UpdateRuleBuilder fromVersion(String version) {
        UpdateRuleBuilder updateRuleBuilder = new UpdateRuleBuilder().fromVersion(version);
        updateRules.add(updateRuleBuilder);
        return updateRuleBuilder;
    }

    public UpdatePlanBuilder config(UpdatePlanConfig config) {
        this.config = config;
        return this;
    }

    public UpdatePlanConfig config() {
        if(this.config == null) {
            config(new UpdatePlanConfig());

        }
        return config;
    }

    public UpdatePlan build() {
        UpdatePlan updatePlan = new UpdatePlan();

        if(this.config == null) {
            updatePlan.setConfig(new UpdatePlanConfig());
        } else {
            updatePlan.setConfig(this.config);
        }

        for(UpdateRuleBuilder urb : updateRules) {
            UpdateRule rule = new UpdateRule(updatePlan);
            rule.setAction(urb.action);
            rule.setComment(urb.dbUpdateStatus.getComment());
            rule.setReference(urb.dbUpdateStatus.getReference());
            rule.setFromVersion(Version.valueOf(urb.dbUpdateStatus.getFromVersion()));
            rule.setTargetVersion(Version.valueOf(urb.dbUpdateStatus.getToVersion()));
            updatePlan.getRules().add(rule);
        }
        return updatePlan;
    }

    private static final String namePattern = "^(.+)_to_(.+).sql$";

    private static final Pattern nameRegex = Pattern.compile(namePattern);

    public UpdatePlanBuilder importFromSqlDir(Path scriptsDirectory) throws IOException {

        Files.list(scriptsDirectory)
                .filter(path -> path.endsWith(".sql") && Files.isRegularFile(path)).
                forEach(path -> {


                    //parse file name in order to get versions
                    String filename = path.getFileName().toString();
                    Matcher matcher  = nameRegex.matcher(filename);

                    if(matcher.matches()) {

                        //create a rule that runs the script
                        fromVersion(matcher.group(1))
                                .toVersion(matcher.group(2))
                                .withReference(path.toString())
                                .action( rule -> {
                                    try {
                                        Callable<Connection> connectionProvider = rule.getPlan().getConfig().get(UpdatePlanConfig.CONNECTION_PROVIDER);
                                        Connection conn = connectionProvider.call();
                                        Reader reader = new InputStreamReader(path.toUri().toURL().openStream());
                                        new SqlScriptRunner(conn).run(filename, reader);
                                    } catch(Exception e) {
                                        LoggerFactory.getLogger(SqlScriptRunner.class).error("script execution failure", e);
                                        //TODO crash the party
                                    }
                                });

                    } else {
                        LOGGER.warn("invalid sql filename (expected pattern : \""+namePattern+"\") : " + filename);
                    }
                });

        return this;
    }
    public UpdateRunner sqlRunner(Connection connection) {
        config().setConnection(connection);
        return sqlRunner();
    }

    public UpdateRunner sqlRunner(DataSource ds) {
        config().setDataSource(ds);
        return sqlRunner();
    }

    public UpdateRunner sqlRunner(Callable<Connection> connectionProvider) {
        config().setConnectionProvider(connectionProvider);
        return sqlRunner();
    }

    public UpdateRunner sqlRunner() {
        UpdatePlan updatePlan = build();
        Callable<Connection> provider = updatePlan.getConfig().get(UpdatePlanConfig.CONNECTION_PROVIDER);
        final Dao dao;
        try {
            dao = new H2Dao(provider.call());
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        updatePlan.getConfig().set("dao", dao);
        return new UpdateRunner(build(), dao);
    }
}
