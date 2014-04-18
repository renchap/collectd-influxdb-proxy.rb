require 'bundler/setup'
Bundler.require(:default)

class InfluxDBProxy
  def self.influx_client
    @@idb_client ||= InfluxDB::Client.new 'collectd', :async => true, :time_precision => 's', :host => 'localhost', :port => 8086, :username => 'collectd', :password => 'collectd'
  end

  def self.send_value(name, time, value)
    self.influx_client.write_point(name, { :value => value, :time => time })
  end

  def self.call(env)
    request = Rack::Request.new env

    r = JSON.parse(request.body.read)
    r.each do |x|
      base_name = x['host'] + '.' + x['plugin']
      base_name += '.' + x['plugin_instance'] unless x['plugin_instance'].empty?
      base_name += '.' + x['type']
      base_name += '.' + x['type_instance'] unless x['type_instance'].empty?

      x['dstypes'].each_with_index do |type, i|
        unless type == 'counter' or type == 'gauge' or type == 'derive'
          puts "TYPE: "+type+" - "+base_name+" : "+x['values'].join(',')
          next
        end
        name = base_name + '.' + x['dsnames'][i]
        value = x['values'][i]
        
        if value.nil?
          puts "NIL: "+name
          next
        end
        #puts "#{x['time']} #{name} : #{x['values'][i].to_s}"
        self.send_value(name, x['time'].to_i, value)
      end

    end 

    # Returns a 200 to collectd
    [200, {}, ['ok']]
  end
end

Rack::Server.start :Port => 9010, :app => InfluxDBProxy
