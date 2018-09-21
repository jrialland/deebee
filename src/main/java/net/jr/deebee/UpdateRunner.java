package net.jr.deebee;

import com.github.zafarkhaja.semver.Version;
import net.jr.deebee.util.path.AStarPathFinder;
import net.jr.deebee.dao.Dao;
import net.jr.deebee.model.DbUpdateLog;
import net.jr.deebee.model.DbUpdateStatus;
import net.jr.deebee.model.UpdatePlan;
import net.jr.deebee.model.UpdateRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class UpdateRunner implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateRunner.class);

    private UpdatePlan updatePlan;

    private Dao dao;

    public UpdateRunner(UpdatePlan updatePlan, Dao dao) {
        setUpdatePlan(updatePlan);
        setDao(dao);
    }

    public void setUpdatePlan(UpdatePlan updatePlan) {
        this.updatePlan = updatePlan;
    }

    public void setDao(Dao dao) {
        this.dao = dao;
    }

    private List<UpdateRule> findSteps(String strFrom, String strTo) {

        final Version fromVersion = Version.valueOf(strFrom);
        final Version toVersion = Version.valueOf(strTo);

        AStarPathFinder<UpdateRule> pathFinder = new AStarPathFinder<UpdateRule>() {

            @Override
            public Iterable<UpdateRule> getNeighbors(UpdateRule n) {
                return () -> updatePlan
                        .getRules()
                        .stream()
                        .filter(rule -> rule.getFromVersion().equals(n.getTargetVersion()))
                        .iterator();
            }

            @Override
            public boolean isGoalReached(UpdateRule current, UpdateRule goal) {
                return current.getTargetVersion().equals(toVersion);
            }

        };

        List<UpdateRule> path = null;

        Iterator<UpdateRule> startRules = updatePlan
                .getRules()
                .stream()
                .filter(rule -> rule.getFromVersion().equals(fromVersion)).iterator();

        while (path == null && startRules.hasNext()) {
            path = pathFinder.find(startRules.next(), null);
        }

        if (path == null) {
            throw new UpdateFailureException("path not found");
        }

        return path;

    }

    private String getLatestVersionFromRules() {
        return updatePlan.getRules()
                .stream()
                .max(Comparator.comparing(rule -> rule.getTargetVersion())).get()
                .getTargetVersion()
                .toString();
    }

    @Override
    public void run() {

        String targetVersion = getLatestVersionFromRules();
        run(targetVersion);
    }

    public void run(String targetVersion) {

        if (updatePlan == null) {
            throw new IllegalStateException("[updatePlan] has not been defined");
        }

        if (dao == null) {
            throw new IllegalStateException("[dao] has not been defined");
        }


        try {
            dao.begin();
            dao.ensureTablesExist();

            String currentVersion = dao.getCurrentVersionFromDb();
            List<UpdateRule> rules = findSteps(currentVersion, targetVersion);
            logRules(rules);


            for (UpdateRule rule : rules) {

                //TODO add log on start
                addLog("INFO", "Executing " + rule.toString() + " ...");

                //TODO execute action for the rule
                rule.getAction().resolve(rule);

                //TODO update state
                DbUpdateStatus updateStatus = new DbUpdateStatus();
                updateStatus.setReference(rule.getReference());
                updateStatus.setComment(rule.getComment());
                updateStatus.setFromVersion(rule.getFromVersion().toString());
                updateStatus.setToVersion(rule.getTargetVersion().toString());
                updateStatus.setCurrent(true);
                updateStatus.setSince(new Date());
                updateStatus.setUser(updatePlan.getProperty("user"));

                dao.insertStatus(updateStatus);
                dao.markAsCurrent(updateStatus);

                //TODO log on end
                addLog("INFO", " ... done");
            }

            //success !
            dao.commit();

        } catch (Exception e) {

            LOGGER.error("database update failure", e);

            //failure : rollback db,
            addLog("ERROR", buildMessage(e));
            dao.rollback();
            throw new UpdateFailureException(e);

        } finally {
            try {
                dao.end();
            } catch (Exception e) {
                LOGGER.error("dao failure", e);
            }
        }
    }

    private static String buildMessage(Exception e) {
        Throwable current = e;
        Set<Throwable> excpts = new HashSet<>();
        excpts.add(e);
        while (current.getCause() != null && !excpts.contains(current.getCause())) {
            current = current.getCause();
            excpts.add(current);
        }

        StringWriter sw = new StringWriter();
        sw.append(current.getClass().getName());
        sw.append(" : ");
        PrintWriter pw = new PrintWriter(sw);
        current.printStackTrace(pw);
        pw.flush();
        String message = sw.toString();
        if (message.length() > 1024) {
            return message.substring(0, 1020) + " ...";
        } else {
            return message;
        }
    }

    private static void logRules(Iterable<UpdateRule> rules) {
        if (LOGGER.isInfoEnabled()) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);

            for (UpdateRule r : rules) {
                pw.println(" - from %s to %s : %s".format(r.getFromVersion().toString(), r.getTargetVersion().toString(), r.getReference()));
                pw.print("\t");
                pw.println(r.toString());
                if (r.getComment() != null && !r.getComment().isEmpty()) {
                    pw.print("\t");
                    pw.println(r.getComment());
                }
                pw.println();
            }

            pw.flush();
            LOGGER.info(sw.toString());
        }
    }

    private void addLog(String level, String message) {
        try {
            DbUpdateLog log = DbUpdateLog.createNew(level, message);
            dao.insertLog(log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
