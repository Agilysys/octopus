module Octopus
  module Migration
    def self.extended(base)
      class << base
        def announce_with_octopus(message)
          announce_without_octopus("#{message} - #{get_current_shard}")
        end

        #alias_method_chain :migrate, :octopus
        alias_method_chain :announce, :octopus
        attr_accessor :current_shard
        attr_accessor :current_group
      end
    end

    def self.included(base)
      base.class_eval do
        def announce_with_octopus(message)
          announce_without_octopus("#{message} - #{get_current_shard}")
        end

        #alias_method_chain :migrate, :octopus
        alias_method_chain :announce, :octopus
        attr_accessor :current_shard
        attr_accessor :current_group
      end
    end

    def using(*args)
      if self.connection().is_a?(Octopus::Proxy)
        args.each do |shard|
          self.connection().check_schema_migrations(shard)
        end

        self.connection().block = true
        self.current_shard = args
        self.connection().current_shard = args
      end

      return self
    end

    def using_group(*groups)
      if self.connection.is_a?(Octopus::Proxy)
        groups.each do |group|
          shards = self.connection.shards_for_group(group) || []

          shards.each do |shard|
            self.connection.check_schema_migrations(shard)
          end
        end

        self.connection.block = true
        self.connection.current_group = groups
        self.current_group = groups
      end

      self
    end

    def get_current_shard
      "Shard: #{ActiveRecord::Base.connection.current_shard()}" if ActiveRecord::Base.connection.respond_to?(:current_shard)
    end
    
    def shards
      shards = Set.new
      conn = ActiveRecord::Base.connection
      return shards unless conn.is_a?(Octopus::Proxy)
      if current_shard.is_a?(Array)
        shards.merge(current_shard)
      else
        shards.add(current_shard)        
      end
      if current_group
        [current_group].flatten.each do |group|
          group_shards = conn.shards_for_group(group)
          shards.merge(group_shards) if group_shards
        end
      end
      shards
    end
    # def migrate_with_octopus(direction)
    #   conn = ActiveRecord::Base.connection
    #   return migrate_without_octopus(direction) unless conn.is_a?(Octopus::Proxy)
    #   self.connection().current_shard = self.current_shard if self.current_shard != nil
    # 
    #   begin
    #     if shards.any?
    #       conn.send_queries_to_multiple_shards(shards.to_a) do
    #         migrate_without_octopus(direction)
    #       end
    #     else
    #       migrate_without_octopus(direction)
    #     end
    #   ensure
    #     conn.clean_proxy
    #   end
    # end
  end
  module MigrationProxy
    def self.included(base)
      base.delegate :shards, :to => :migration
    end    
  end
  module Migrator
    def self.included(base)
      base.alias_method_chain :migrate, :octopus
      base.alias_method_chain :migrated, :octopus
    end
    def migrate_one migration
      begin
        ddl_transaction do
          migration.migrate(@direction)
          record_version_state_after_migrating(migration.version)
        end
      rescue => e
        canceled_msg = ActiveRecord::Base.connection.supports_ddl_transactions? ? "this and " : ""
        raise StandardError, "An error has occurred, #{canceled_msg}all later migrations canceled:\n\n#{e}", e.backtrace
      end      
    end
    def migrated_with_octopus
      self.class.get_all_versions
    end    
    def migrate_with_octopus
      conn = ActiveRecord::Base.connection
      return migrate_without_octopus unless conn.is_a?(Octopus::Proxy)            

      current = migrations.detect { |m| m.version == current_version }
      target = migrations.detect { |m| m.version == @target_version }

      if target.nil? && @target_version && @target_version > 0
        raise ActiveRecord::UnknownMigrationVersionError.new(@target_version)
      end

      start = up? ? 0 : (migrations.index(current) || 0)
      finish = migrations.index(target) || migrations.size - 1
      runnable = migrations[start..finish]

      # skip the last migration if we're headed down, but not ALL the way down
      runnable.pop if down? && target

      ran = []
      runnable.each do |migration|
        ActiveRecord::Base.logger.info "Migrating to #{migration.name} (#{migration.version})" if ActiveRecord::Base.logger
        shards = migration.respond_to?(:shards) ? migration.shards : []
        begin
          if shards.any?
            conn.send_queries_to_multiple_shards(shards.to_a) do
              # On our way up, we skip migrating the ones we've already migrated
              next if up? && migrated.include?(migration.version.to_i)

              # On our way down, we skip reverting the ones we've never migrated
              if down? && !migrated.include?(migration.version.to_i)
                migration.announce 'never migrated, skipping'; migration.write
                next
              end
              
              migrate_one migration
            end
          else
            # On our way up, we skip migrating the ones we've already migrated
            next if up? && migrated.include?(migration.version.to_i)

            # On our way down, we skip reverting the ones we've never migrated
            if down? && !migrated.include?(migration.version.to_i)
              migration.announce 'never migrated, skipping'; migration.write
              next
            end
            migrate_one migration
          end
        ensure
          conn.clean_proxy
        end
      end
      ran
    end
  end
end
if Octopus.rails31?
  ActiveRecord::Migration.send(:include, Octopus::Migration)
else
  ActiveRecord::Migration.extend(Octopus::Migration)
end
ActiveRecord::Migrator.send(:include, Octopus::Migrator)
ActiveRecord::MigrationProxy.send(:include, Octopus::MigrationProxy)
