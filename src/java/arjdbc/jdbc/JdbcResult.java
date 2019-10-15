package arjdbc.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.jruby.*;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * This is a base Result class to be returned as the "raw" result.
 * It should be overridden for specific adapters to manage type maps
 * and provide any additional methods needed.
 */
public class JdbcResult extends RubyObject {
    // Should these be private with accessors?
    protected final RubyArray values;
    protected RubyHash[] tuples;

    protected final RubyJdbcConnection connection;

    protected JdbcResultMeta meta;

    protected JdbcResult(ThreadContext context, RubyClass clazz, RubyJdbcConnection connection, ResultSet resultSet,
                         StatementCache.CacheEntry cacheEntry, boolean arResult) throws SQLException {
        super(context.runtime, clazz);

        values = context.runtime.newArray();
        this.connection = connection;

        if (cacheEntry != null) this.meta = cacheEntry.meta;

        if (this.meta == null) {
            this.meta = new JdbcResultMeta();

            this.meta.arResult = arResult;
            setupColumnTypeMap(context, arResult);

            final ResultSetMetaData resultMetaData = resultSet.getMetaData();
            final int columnCount = resultMetaData.getColumnCount();
            // FIXME: if we support MSSQL we may need to change how we deal with omitting elements
            meta.columnNames = new RubyString[columnCount];
            meta.columnTypes = new int[columnCount];
            extractColumnInfo(context, resultMetaData);

            if (cacheEntry != null) cacheEntry.meta = this.meta;
        }

        processResultSet(context, resultSet);
    }

    protected void setupColumnTypeMap(ThreadContext context, boolean arResult) {
        meta.columnTypeMap = context.nil;
    }

    protected void addToColumnTypeMap(ThreadContext context, ResultSetMetaData resultSetMetaData, int col) throws SQLException {
    }

    protected boolean isBinaryType(final int type) {
        return type == Types.BLOB || type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY;
    }

    /**
     * Build an array of column types
     * @param resultMetaData metadata from a ResultSet to determine column information from
     * @throws SQLException throws error!
     */
    private void extractColumnInfo(ThreadContext context, ResultSetMetaData resultMetaData) throws SQLException {
        final int columnCount = resultMetaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) { // metadata is one-based
            // This appears to not be used by Postgres, MySQL, or SQLite so leaving it off for now
            //name = caseConvertIdentifierForRails(connection, name);
            int colIndex = i - 1;
            meta.columnNames[colIndex] = RubyJdbcConnection.STRING_CACHE.get(context, resultMetaData.getColumnLabel(i));
            meta.columnTypes[colIndex] = resultMetaData.getColumnType(i);
            if (!meta.hasBinary && isBinaryType(meta.columnTypes[colIndex])) meta.hasBinary = true;
            addToColumnTypeMap(context, resultMetaData, i);
        }
    }

    /**
     * @return an array with the column names as Ruby strings
     */
    protected RubyString[] getColumnNames() {
        return meta.columnNames;
    }

    /**
     * Builds an array of hashes with column names to column values
     * @param context current thread context
     */
    protected void populateTuples(final ThreadContext context) {
        int columnCount = meta.columnNames.length;
        tuples = new RubyHash[values.size()];

        for (int i = 0; i < tuples.length; i++) {
            RubyArray currentRow = (RubyArray) values.eltInternal(i);
            RubyHash hash = RubyHash.newHash(context.runtime);
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                hash.fastASet(meta.columnNames[columnIndex], currentRow.eltInternal(columnIndex));
            }
            tuples[i] = hash;
        }
    }

    private RubyClass getBinaryDataClass(final ThreadContext context) {
        return ((RubyModule) context.runtime.getModule("ActiveModel").getConstantAt("Type")).getClass("Binary").getClass("Data");
    }

    /**
     * Does the heavy lifting of turning the JDBC objects into Ruby objects
     * @param context current thread context
     * @param resultSet the set of results we are converting
     * @throws SQLException throws!
     */
    private void processResultSet(final ThreadContext context, final ResultSet resultSet) throws SQLException {
        Ruby runtime = context.runtime;
        int columnCount = meta.columnNames.length;
        RubyClass BinaryDataClass = meta.hasBinary && meta.arResult ? getBinaryDataClass(context) : null;

        while (resultSet.next()) {
            final IRubyObject[] row = new IRubyObject[columnCount];

            for (int i = 0; i < columnCount; i++) {
                int colType = meta.columnTypes[i];

                IRubyObject val = connection.jdbcToRuby(context, runtime, i + 1, colType, resultSet); // Result Set is 1 based

                // wrapp binary data for AR when necessary
                if (BinaryDataClass != null && isBinaryType(colType) && val != context.nil) {
                    val = BinaryDataClass.newInstance(context, val, Block.NULL_BLOCK);
                }

                row[i] = val;
            }

            values.append(RubyArray.newArrayNoCopy(context.runtime, row));
        }
    }

    /**
     * Creates an <code>ActiveRecord::Result</code> with the data from this result
     * @param context current thread context
     * @return ActiveRecord::Result object with the data from this result set
     * @throws SQLException can be caused by postgres generating its type map
     */
    public IRubyObject toARResult(final ThreadContext context) throws SQLException {
        final RubyClass Result = RubyJdbcConnection.getResult(context.runtime);
        // FIXME: Is this broken?  no copy of an array AR::Result can modify?  or should it be frozen?
        final RubyArray rubyColumnNames = RubyArray.newArrayNoCopy(context.runtime, getColumnNames());
        return Result.newInstance(context, rubyColumnNames, values, meta.columnTypeMap, Block.NULL_BLOCK);
    }
}
