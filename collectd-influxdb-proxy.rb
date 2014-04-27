require 'bundler/setup'
Bundler.require(:default)

class InfluxDBProxy
  def self.influx_client
    @@idb_client ||= InfluxDB::Client.new 'collectd', :async => true, :time_precision => 's', :host => 'localhost', :port => 8086, :username => 'collectd', :password => 'collectd'
  end

  def self.send_value(name, time, value)
    self.influx_client.write_point(name, { :value => value, :time => time })
  end

  def self.redis_client
    @@redis_client ||= Redis.new( :host => "localhost", :port => "6379")
  end

  def self.call(env)
    request = Rack::Request.new env

    @@names ||= Array.new

    r = JSON.parse(request.body.read)
    r.each do |x|
      base_name = x['host'].clone
      base_name.prepend(x['plugin'] + '.')
      base_name.prepend(x['plugin_instance'] + '.') unless x['plugin_instance'].empty?
      base_name.prepend(x['type'] + '.')
      base_name.prepend(x['type_instance'] + '.') unless x['type_instance'].empty?

      x['dstypes'].each_with_index do |dstype, i|
        unless ['counter', 'gauge', 'derive'].include?(dstype)
          puts "TYPE: "+dstype+" - "+base_name+" : "+x['values'].join(',')
          next
        end

        dsname = x['dsnames'][i]
        name = dsname + '.' + base_name

        value = x['values'][i]

        if value.nil?
          puts "NIL: " + name
          next
        end

        #puts "#{x['time']} #{name} : #{x['values'][i].to_s}"
        self.send_value(name, x['time'].to_i, value)

        unless @@names.include?(name)
          @@names << name

          # Insert the metrics names and metadata into redis
          self.redis_client.sadd('influx_series_names', name)
          obj = {
            'dstype' => dstype,
            'dsname' => dsname,
          }
          [:interval, :host, :plugin, :plugin_instance, :type, :type_instance].each do |f|
            obj[f] = x[f.to_s]
          end

          self.redis_client.set('influx_serie_'+name, obj.to_json)
        end
      end

    end

    # Returns a 200 to collectd
    [200, {}, ['ok']]
  end
end

Rack::Server.start :Port => 9010, :app => InfluxDBProxy
