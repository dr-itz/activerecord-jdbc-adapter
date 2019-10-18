# frozen_string_literal: true

require 'active_record/connection_adapters/statement_pool'

module ArJdbc
  module Abstract
    module StatementCache
      def initialize(*args) # (connection, logger, config)
        super

        # Only say we support the statement cache if we are using prepared statements
        # and have a max number of statements defined
        statement_limit = prepared_statements ?
          self.class.type_cast_config_to_integer(config[:statement_limit]) ||
            ActiveRecord::ConnectionAdapters::StatementPool::DEFAULT_STATEMENT_LIMIT
          : 0
        @statements = Struct.new(:cache).new @connection.setup_statement_cache(statement_limit)
      end

      # Clears the prepared statements cache.
      def clear_cache!
        @connection.clear_statement_cache
      end

      private

      # This should be overridden by the adapter if the sql itself
      # is not enough to make the key unique
      # for testing only
      def sql_key(sql)
        [ sql, nil ]
      end
    end
  end
end
