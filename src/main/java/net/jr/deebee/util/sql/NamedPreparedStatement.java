package net.jr.deebee.util.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class NamedPreparedStatement implements PreparedStatement {

    private static final int[] EMPTY_INT_ARRAY = new int[]{};

    private static class ParsedQuery {
        private String query;
        private Map<String, int[]> params;
    }

    private String originalQuery;

    private PreparedStatement preparedStatement;

    private ParsedQuery parsedQuery;

    private boolean allowMissingParameters = true;

    private NamedPreparedStatement(PreparedStatement preparedStatement, ParsedQuery parsedQuery, String originalQuery) {
        this.preparedStatement = preparedStatement;
        this.parsedQuery = parsedQuery;
        this.originalQuery = originalQuery;
    }

    public static NamedPreparedStatement create(Connection connection, String query) throws SQLException {
        ParsedQuery p = parse(query.trim());
        return new NamedPreparedStatement(connection.prepareStatement(p.query), p, query);
    }

    public static NamedPreparedStatement create(Connection connection, String query, int options) throws SQLException {
        ParsedQuery p = parse(query.trim());
        return new NamedPreparedStatement(connection.prepareStatement(p.query, options), p, query);
    }

    public void setAllowMissingParameters(boolean allowMissingParameters) {
        this.allowMissingParameters = allowMissingParameters;
    }

    public boolean isAllowMissingParameters() {
        return allowMissingParameters;
    }

    private static final ParsedQuery parse(String query) {

        Map<String, List<Integer>> paramMap = new TreeMap<>();

        int length = query.length();
        StringBuffer parsedQuery = new StringBuffer(length);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int index = 1;

        for (int i = 0; i < length; i++) {
            char c = query.charAt(i);
            if (inSingleQuote) {
                if (c == '\'') {
                    inSingleQuote = false;
                }
            } else if (inDoubleQuote) {
                if (c == '"') {
                    inDoubleQuote = false;
                }
            } else {
                if (c == '\'') {
                    inSingleQuote = true;
                } else if (c == '"') {
                    inDoubleQuote = true;
                } else if (c == ':' && i + 1 < length &&
                        Character.isJavaIdentifierStart(query.charAt(i + 1))) {
                    int j = i + 2;
                    while (j < length && Character.isJavaIdentifierPart(query.charAt(j))) {
                        j++;
                    }
                    String name = query.substring(i + 1, j);
                    c = '?'; // replace the parameter with a question mark
                    i += name.length(); // skip past the end if the parameter

                    List<Integer> indexList = paramMap.get(name);
                    if (indexList == null) {
                        indexList = new ArrayList<>();
                        paramMap.put(name, indexList);
                    }
                    indexList.add(new Integer(index));

                    index++;
                }
            }
            parsedQuery.append(c);
        }

        ParsedQuery p = new ParsedQuery();
        p.query = parsedQuery.toString();
        p.params = new TreeMap<>();

        // replace the lists of Integer objects with arrays of ints
        paramMap.entrySet().stream().forEach(entry -> {
            p.params.put(entry.getKey(), entry.getValue().stream().mapToInt(Integer::intValue).toArray());
        });

        return p;
    }

    private int[] getPositions(String param) {
        if (parsedQuery.params.containsKey(param)) {
            return parsedQuery.params.get(param);
        } else {

            if (allowMissingParameters) {

                return EMPTY_INT_ARRAY;

            } else {
                throw new IllegalArgumentException("No Such named parameter : " + param);
            }
        }
    }

    public ResultSet executeQuery() throws SQLException {
        return preparedStatement.executeQuery();
    }

    public int executeUpdate() throws SQLException {
        return preparedStatement.executeUpdate();
    }

    @Override
    public void setNull(int i, int i1) throws SQLException {
        preparedStatement.setNull(i, i1);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        preparedStatement.setBoolean(i, b);
    }

    @Override
    public void setByte(int i, byte b) throws SQLException {
        preparedStatement.setByte(i, b);
    }

    @Override
    public void setShort(int i, short i1) throws SQLException {
        preparedStatement.setShort(i, i1);
    }

    @Override
    public void setInt(int i, int i1) throws SQLException {
        preparedStatement.setInt(i, i1);
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        preparedStatement.setLong(i, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        preparedStatement.setFloat(i, v);
    }


    @Override
    public void setDouble(int i, double v) throws SQLException {
        preparedStatement.setDouble(i, v);
    }


    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        preparedStatement.setBigDecimal(i, bigDecimal);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        preparedStatement.setString(i, s);
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        preparedStatement.setBytes(i, bytes);
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        preparedStatement.setDate(i, date);
    }

    @Override
    public void setTime(int i, Time time) throws SQLException {
        preparedStatement.setTime(i, time);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        preparedStatement.setTimestamp(i, timestamp);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream, i1);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int i, InputStream inputStream, int i1) throws SQLException {
        preparedStatement.setUnicodeStream(i, inputStream, i1);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream, i1);
    }

    public NamedPreparedStatement setNull(String param, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setNull(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setBoolean(String param, boolean i) throws SQLException {
        for (int position : getPositions(param)) {
            setBoolean(position, i);
        }
        return this;
    }

    public NamedPreparedStatement setByte(String param, byte i) throws SQLException {
        for (int position : getPositions(param)) {
            setByte(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setShort(String param, short i) throws SQLException {
        for (int position : getPositions(param)) {
            setShort(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setInt(String param, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setInt(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setLong(String param, long i) throws SQLException {
        for (int position : getPositions(param)) {
            setLong(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setFloat(String param, float i) throws SQLException {
        for (int position : getPositions(param)) {
            setFloat(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setDouble(String param, double i) throws SQLException {
        for (int position : getPositions(param)) {
            setDouble(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setBigDecimal(String param, BigDecimal i) throws SQLException {
        for (int position : getPositions(param)) {
            setBigDecimal(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setString(String param, String i) throws SQLException {
        for (int position : getPositions(param)) {
            setString(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setBytes(String param, byte[] i) throws SQLException {
        for (int position : getPositions(param)) {
            setBytes(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setDate(String param, Date i) throws SQLException {
        for (int position : getPositions(param)) {
            setDate(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setTime(String param, Time i) throws SQLException {
        for (int position : getPositions(param)) {
            setTime(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setTimestamp(String param, Timestamp i) throws SQLException {
        for (int position : getPositions(param)) {
            setTimestamp(position, i);
        }
        return this;
    }


    public NamedPreparedStatement setAsciiStream(String param, InputStream inputStream, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setAsciiStream(position, inputStream, i);
        }
        return this;
    }


    public NamedPreparedStatement setUnicodeStream(String param, InputStream inputStream, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setUnicodeStream(position, inputStream, i);
        }
        return this;
    }


    public NamedPreparedStatement setBinaryStream(String param, InputStream inputStream, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setBinaryStream(position, inputStream, i);
        }
        return this;
    }

    public void clearParameters() throws SQLException {
        preparedStatement.clearParameters();
    }

    public void setObject(int i, Object o, int i1) throws SQLException {
        preparedStatement.setObject(i, o, i1);
    }


    public void setObject(int i, Object o) throws SQLException {
        preparedStatement.setObject(i, o);
    }

    public NamedPreparedStatement setObject(String param, Object o, int i) throws SQLException {
        for (int position : getPositions(param)) {
            setObject(position, o, i);
        }
        return this;
    }

    public void setObject(String param, Object i) throws SQLException {
        for (int position : getPositions(param)) {
            setObject(position, i);
        }
    }

    public boolean execute() throws SQLException {
        return preparedStatement.execute();
    }

    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
    }

    public void setCharacterStream(int i, Reader reader, int i1) throws SQLException {
        preparedStatement.setCharacterStream(i, reader, i1);
    }

    public void setRef(int i, Ref ref) throws SQLException {
        preparedStatement.setRef(i, ref);
    }

    public void setBlob(int i, Blob blob) throws SQLException {
        preparedStatement.setBlob(i, blob);
    }

    public void setClob(int i, Clob clob) throws SQLException {
        preparedStatement.setClob(i, clob);
    }

    public void setArray(int i, Array array) throws SQLException {
        preparedStatement.setArray(i, array);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        preparedStatement.setDate(i, date, calendar);
    }

    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        preparedStatement.setTime(i, time, calendar);
    }

    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        preparedStatement.setTimestamp(i, timestamp, calendar);
    }

    public void setNull(int i, int i1, String s) throws SQLException {
        preparedStatement.setNull(i, i1, s);
    }

    public void setURL(int i, URL url) throws SQLException {
        preparedStatement.setURL(i, url);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return preparedStatement.getParameterMetaData();
    }

    public void setRowId(int i, RowId rowId) throws SQLException {
        preparedStatement.setRowId(i, rowId);
    }

    public void setNString(int i, String s) throws SQLException {
        preparedStatement.setNString(i, s);
    }

    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setNCharacterStream(i, reader, l);
    }

    public void setNClob(int i, NClob nClob) throws SQLException {
        preparedStatement.setNClob(i, nClob);
    }

    public void setClob(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setClob(i, reader, l);
    }

    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setBlob(i, inputStream, l);
    }

    public void setNClob(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setNClob(i, reader, l);
    }

    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        preparedStatement.setSQLXML(i, sqlxml);
    }

    public void setObject(int i, Object o, int i1, int i2) throws SQLException {
        preparedStatement.setObject(i, o, i1, i2);
    }

    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream, l);
    }

    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream, l);
    }

    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setCharacterStream(i, reader, l);
    }

    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream);
    }

    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream);
    }

    public void setCharacterStream(int i, Reader reader) throws SQLException {
        preparedStatement.setCharacterStream(i, reader);
    }

    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        preparedStatement.setNCharacterStream(i, reader);
    }

    public void setClob(int i, Reader reader) throws SQLException {
        preparedStatement.setClob(i, reader);
    }

    public void setBlob(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setBlob(i, inputStream);
    }

    public void setNClob(int i, Reader reader) throws SQLException {
        preparedStatement.setNClob(i, reader);
    }

    public void setObject(int i, Object o, SQLType sqlType, int i1) throws SQLException {
        preparedStatement.setObject(i, o, sqlType, i1);
    }

    public void setObject(int i, Object o, SQLType sqlType) throws SQLException {
        preparedStatement.setObject(i, o, sqlType);
    }

    public long executeLargeUpdate() throws SQLException {
        return preparedStatement.executeLargeUpdate();
    }

    public ResultSet executeQuery(String s) throws SQLException {
        return preparedStatement.executeQuery(s);
    }

    public int executeUpdate(String s) throws SQLException {
        return preparedStatement.executeUpdate(s);
    }

    public void close() throws SQLException {
        preparedStatement.close();
    }

    public int getMaxFieldSize() throws SQLException {
        return preparedStatement.getMaxFieldSize();
    }

    public void setMaxFieldSize(int i) throws SQLException {
        preparedStatement.setMaxFieldSize(i);
    }

    public int getMaxRows() throws SQLException {
        return preparedStatement.getMaxRows();
    }

    public void setMaxRows(int i) throws SQLException {
        preparedStatement.setMaxRows(i);
    }

    public void setEscapeProcessing(boolean b) throws SQLException {
        preparedStatement.setEscapeProcessing(b);
    }

    public int getQueryTimeout() throws SQLException {
        return preparedStatement.getQueryTimeout();
    }

    public void setQueryTimeout(int i) throws SQLException {
        preparedStatement.setQueryTimeout(i);
    }

    public void cancel() throws SQLException {
        preparedStatement.cancel();
    }

    public SQLWarning getWarnings() throws SQLException {
        return preparedStatement.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        preparedStatement.clearWarnings();
    }

    public void setCursorName(String s) throws SQLException {
        preparedStatement.setCursorName(s);
    }

    public boolean execute(String s) throws SQLException {
        return preparedStatement.execute(s);
    }

    public ResultSet getResultSet() throws SQLException {
        return preparedStatement.getResultSet();
    }

    public int getUpdateCount() throws SQLException {
        return preparedStatement.getUpdateCount();
    }

    public boolean getMoreResults() throws SQLException {
        return preparedStatement.getMoreResults();
    }

    public void setFetchDirection(int i) throws SQLException {
        preparedStatement.setFetchDirection(i);
    }

    public int getFetchDirection() throws SQLException {
        return preparedStatement.getFetchDirection();
    }

    public void setFetchSize(int i) throws SQLException {
        preparedStatement.setFetchSize(i);
    }

    public int getFetchSize() throws SQLException {
        return preparedStatement.getFetchSize();
    }

    public int getResultSetConcurrency() throws SQLException {
        return preparedStatement.getResultSetConcurrency();
    }

    public int getResultSetType() throws SQLException {
        return preparedStatement.getResultSetType();
    }

    public void addBatch(String s) throws SQLException {
        preparedStatement.addBatch(s);
    }

    public void clearBatch() throws SQLException {
        preparedStatement.clearBatch();
    }

    public int[] executeBatch() throws SQLException {
        return preparedStatement.executeBatch();
    }

    public Connection getConnection() throws SQLException {
        return preparedStatement.getConnection();
    }

    public boolean getMoreResults(int i) throws SQLException {
        return preparedStatement.getMoreResults(i);
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return preparedStatement.getGeneratedKeys();
    }

    public int executeUpdate(String s, int i) throws SQLException {
        return preparedStatement.executeUpdate(s, i);
    }

    public int executeUpdate(String s, int[] ints) throws SQLException {
        return preparedStatement.executeUpdate(s, ints);
    }

    public int executeUpdate(String s, String[] strings) throws SQLException {
        return preparedStatement.executeUpdate(s, strings);
    }

    public boolean execute(String s, int i) throws SQLException {
        return preparedStatement.execute(s, i);
    }

    public boolean execute(String s, int[] ints) throws SQLException {
        return preparedStatement.execute(s, ints);
    }

    public boolean execute(String s, String[] strings) throws SQLException {
        return preparedStatement.execute(s, strings);
    }

    public int getResultSetHoldability() throws SQLException {
        return preparedStatement.getResultSetHoldability();
    }

    public boolean isClosed() throws SQLException {
        return preparedStatement.isClosed();
    }

    public void setPoolable(boolean b) throws SQLException {
        preparedStatement.setPoolable(b);
    }

    public boolean isPoolable() throws SQLException {
        return preparedStatement.isPoolable();
    }

    public void closeOnCompletion() throws SQLException {
        preparedStatement.closeOnCompletion();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return preparedStatement.isCloseOnCompletion();
    }

    public long getLargeUpdateCount() throws SQLException {
        return preparedStatement.getLargeUpdateCount();
    }

    public void setLargeMaxRows(long l) throws SQLException {
        preparedStatement.setLargeMaxRows(l);
    }

    public long getLargeMaxRows() throws SQLException {
        return preparedStatement.getLargeMaxRows();
    }

    public long[] executeLargeBatch() throws SQLException {
        return preparedStatement.executeLargeBatch();
    }

    public long executeLargeUpdate(String s) throws SQLException {
        return preparedStatement.executeLargeUpdate(s);
    }

    public long executeLargeUpdate(String s, int i) throws SQLException {
        return preparedStatement.executeLargeUpdate(s, i);
    }

    public long executeLargeUpdate(String s, int[] ints) throws SQLException {
        return preparedStatement.executeLargeUpdate(s, ints);
    }

    public long executeLargeUpdate(String s, String[] strings) throws SQLException {
        return preparedStatement.executeLargeUpdate(s, strings);
    }

    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return preparedStatement.unwrap(aClass);
    }

    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return preparedStatement.isWrapperFor(aClass);
    }

    public void setParameters(Map<String, ?> parameters) throws SQLException {
        for (Map.Entry<String, ?> entry : parameters.entrySet()) {
            setObject(entry.getKey(), entry.getValue());
        }
    }

    public long getUniqueGeneratedKey() throws SQLException {
        ResultSet rs = getGeneratedKeys();
        try {
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new IllegalStateException("No keys were generated");
            }
        } finally {
            rs.close();
        }
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public String getPreparedQuery() {
        return parsedQuery.query;
    }

    public Set<String> getParameterNames() {
        return Collections.unmodifiableSet(parsedQuery.params.keySet());
    }

}
