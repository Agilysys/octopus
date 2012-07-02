module Octopus::Migrator  
  def self.extended(base)
    class << base
      alias_method_chain :migrate, :octopus
      attr_accessor :current_shard
    end
  end

  #ActiveRecord::Base.connection.shards.keys
  
  def migrate_with_octopus(migrations_path, target_version = nil)
    conn = ActiveRecord::Base.connection
    return migrate_without_octopus(migrations_path, target_version = nil) unless conn.is_a?(Octopus::Proxy)
    ActiveRecord::Migration.connection().current_shard = ActiveRecord::Base.connection.shards
    
    puts "Current SHARD = #{Octopus.shards}"
    groups = conn.instance_variable_get(:@groups)
    
    begin
      if conn.current_group.is_a?(Array)
        puts "GROUP"
        conn.current_group.each { |group| conn.send_queries_to_multiple_shards(groups[group]) { migrate_without_octopus(migrations_path, target_version = nil) } } 
      elsif conn.current_group.is_a?(Symbol)       
        puts "GROUP"
        conn.send_queries_to_multiple_shards(groups[conn.current_group]) { migrate_without_octopus(migrations_path, target_version = nil) }     
      elsif conn.current_shard.is_a?(Array)
        puts "migrate_with_octopus ARRAY current shard"
        puts conn.current_shard
        conn.send_queries_to_multiple_shards(conn.current_shard) { migrate_without_octopus(migrations_path, target_version = nil) }     
      else
        raise "BOOO"
        migrate_without_octopus(migrations_path, target_version = nil)
      end
    ensure
      conn.clean_proxy()
    end
  end

end

ActiveRecord::Migrator.extend(Octopus::Migrator)
