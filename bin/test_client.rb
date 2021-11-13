#!/usr/bin/env ruby
require 'cotcube-helpers'

s = Cotcube::Helpers::DataClient.new

print "Ping => "
puts "#{s.send_command({ command: 'ping' })}"

raw  = s.get_historical(contract: 'GCZ21', interval: :min15)

result = JSON.parse(raw, symbolize_names: true)
if result[:error].zero?
  result[:result][-20..].each {|z| p z.slice(*%i[time open high low close volume]) } 
else
  puts "Some ERROR occured: #{result}"
end

# Please test this during business hours
bars = [ ] 
id = s.start_persistent(contract: 'GCZ21', type: :realtimebars) {|bar| puts "Got #{bar}"; bars << bar }
sleep 20
s.stop_persistent(contract: 'GCZ21', type: :realtimebars)

puts "Received #{bars.count} bars. Exiting now..."
