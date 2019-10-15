package arjdbc.jdbc;

import org.jruby.RubyString;
import org.jruby.runtime.builtin.IRubyObject;

public class JdbcResultMeta {
    public int[] columnTypes;
    public RubyString[] columnNames;
    public IRubyObject columnTypeMap;
    public boolean arResult;
    public boolean hasBinary;
}
