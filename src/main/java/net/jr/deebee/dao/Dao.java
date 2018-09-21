package net.jr.deebee.dao;

import net.jr.deebee.model.DbUpdateLog;
import net.jr.deebee.model.DbUpdateStatus;

public interface Dao {

    String getCurrentVersionFromDb() throws Exception;

    void insertLog(DbUpdateLog updateLog) throws Exception;

    void insertStatus(DbUpdateStatus updateStatus) throws Exception;

    void markAsCurrent(DbUpdateStatus updateStatus) throws Exception;

    void begin() throws Exception;

    void ensureTablesExist() throws Exception;

    void commit() throws Exception;

    void rollback();

    void end() throws Exception;
}
