/*
 ***** BEGIN LICENSE BLOCK *****
 * Copyright (c) 2019 Daniel Ritz <daniel.ritz@shellybits.ch>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ***** END LICENSE BLOCK *****/
package arjdbc.jdbc;

import org.jruby.RubyArray;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class StatementCache {
    private LinkedHashMap<StatementCacheKey, PreparedStatement> cache;
    private boolean enabled;

    public StatementCache(int capacity) {
        enabled = capacity > 0;
        if (!enabled) return;

        this.cache = new LinkedHashMap<StatementCacheKey, PreparedStatement>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<StatementCacheKey, PreparedStatement> eldest) {
                boolean remove = size() > capacity;
                if (remove) {
                    evict(null, eldest.getValue());
                }
                return remove;
            }
        };
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void clear() {
        if (!enabled) return;

        cache.forEach(StatementCache::evict);
        cache.clear();
    }

    public boolean put(StatementCacheKey key, PreparedStatement value) {
        if (!enabled) return false;

        evict(null, cache.put(key, value));
        return true;
    }

    public PreparedStatement get(StatementCacheKey key) {
        if (!enabled) return null;

        return cache.get(key);
    }

    static void evict(StatementCacheKey unused, PreparedStatement ps) {
        if (ps == null) return;
        try {
            ps.close();
        } catch (SQLException ignored) { }
    }

    // for testing only
    public boolean include(IRubyObject key) {
        if (!enabled) return false;

        IRubyObject sql = key;
        IRubyObject schema = null;
        if (key instanceof RubyArray) {
            sql = ((RubyArray) key).eltOk(0);
            schema = ((RubyArray) key).eltOk(1);
        }
        return cache.containsKey(new StatementCacheKey(sql, schema));
    }

    /**
     * A key in the statement cache
     */
    public static class StatementCacheKey
    {
        private ByteList sql;
        private ByteList schema;

        public StatementCacheKey(IRubyObject sql, IRubyObject schema) {
            this.sql = sql.convertToString().getByteList();
            this.schema = schema == null || schema.isNil() ? null : schema.convertToString().getByteList();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatementCacheKey that = (StatementCacheKey) o;
            return sql.equals(that.sql) && Objects.equals(schema, that.schema);
        }

        @Override
        public int hashCode() {
            int hash = sql.hashCode();
            return schema == null ? hash : hash * 31 + schema.hashCode();
        }

        @Override
        public String toString() {
            return sql.toString() + " : " + (schema != null ? schema.toString() : "nil");
        }
    }
}
