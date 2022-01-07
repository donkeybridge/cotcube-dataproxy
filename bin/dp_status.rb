#!/usr/bin/env ruby
require 'json'
require 'timeout'
require 'cotcube-helpers'


finish = Proc.new{|a='out of business hours',b=0| puts "#{a}"; exit b }

now = Cotcube::Helpers::CHICAGO.now
# dont check between 1600 and 1659
finish.call if now.hour == 16
#dont check sunday before 16
finish.call if now.wday == 0 and now.hour < 16
# dont check saturdays
finish.call if now.wday == 6
# dont check friday past 16
finish.call if now.wday == 5 and now.hour > 16

breakfast = JSON.parse(`/usr/local/bin/cccache breakfast 55555557`, symbolize_names: true)
contract  = 'ESH22' #breakfast[:payload].first[:contract]
result    = [] 
begin 
  Timeout.timeout(10) do 
    result = Cotcube::Helpers::DataClient.new.get_historical(contract: contract, interval: :min30, duration: '2_D' )
  end
rescue Timeout::Error
  finish.call('Timeout: No response from dataproxy', 1)
end

finish.call('Could not parse response from dataproxy', 1)  unless (result = JSON.parse(result, symbolize_names: true) rescue false)

finish.call('invalid result, no base contained', 1) if result[:base].nil?
finish.call('invalid result, base contained to few records', 1) if result[:base].size < 10
finish.call('OK',0) 


