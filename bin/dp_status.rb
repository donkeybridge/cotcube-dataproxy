#!/usr/bin/env ruby
require 'json'
require 'timeout'
require 'cotcube-helpers'


now = Cotcube::Helpers::CHICAGO.now
exit 0 if now.hour == 16
exit 0 if now.wday == 0 and now.hour < 16
exit 0 if now.wday == 6
exit 0 if now.wday == 5 and now.hour > 16

breakfast = JSON.parse(`/usr/local/bin/cccache breakfast 55555555`, symbolize_names: true)
contract  = breakfast[:payload].first[:contract]
result    = [] 
Timeout.timeout(10) do 
  result    = Cotcube::Helpers::DataClient.new.get_historical(contract: contract, interval: :min30, duration: '2_D' )
rescue Timeout::Error
  exit 1
end

result = JSON.parse(result, symbolize_names: true)

exit 1 if result[:base].nil?
exit 1 if result[:base].size < 10
exit 0 


