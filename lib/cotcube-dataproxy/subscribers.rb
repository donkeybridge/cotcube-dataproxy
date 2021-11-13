module Cotcube
  class DataProxy

    def __int2hex__(id)
      tmp = id.to_s(16) rescue nil
      return nil if tmp.nil?
      tmp.prepend('0') while tmp.length < 7
      return tmp
    end
    
    def spawn_message_subscribers
      @msg_queue = Queue.new
      @depth_queue = Queue.new

      ib.subscribe(:MarketDataType, :TickRequestParameters){|msg| log "#{msg.class}\t#{msg.data.inspect}".colorize(:yellow) }

      @msg_subscriber = ib.subscribe(
        :Alert,
        :ContractData, :ContractDataEnd, :BondContractData,
        :TickGeneric, :TickString, :TickPrice, :TickSize,
        :HistoricalData,
        :RealTimeBar
      ) do |msg|

        @msg_queue << msg
      end

      @depth_subscriber = ib.subscribe( :MarketDepth ) {|msg| @depth_queue << msg}

      @msg_subscriber_thread = Thread.new do
        loop do
          msg = @msg_queue.pop

          data             = msg.data
          data[:time]      = msg.created_at.strftime('%H:%M:%S')
          data[:timestamp] = (msg.created_at.to_time.to_f * 1000).to_i
          __id__           = __int2hex__(data[:request_id])

          case msg

          when IB::Messages::Incoming::HistoricalData
            client_success(requests[__id__]) { msg.results }
            req_mon.synchronize { requests.delete(__id__) }

          when IB::Messages::Incoming::Alert # Alert
            __id__ = __int2hex__(data[:error_id])
            case data[:code]
            when 162
              log("ALERT 162:".light_red + ' MISSING MARKET DATA PERMISSION')
            when 201
              log("ALERT 201:".light_red + ' DUPLICATE OCA_GROUP')
            else
              log("ALERT #{data[:code]}:".light_red + "        #{data[:message]}")
            end
            data[:msg_type] = 'alert'
            client_fail(requests[__id__]) {data} unless requests[__id__].nil?
            log data

          when IB::Messages::Incoming::ContractData
            req_mon.synchronize do
              requests[__id__][:result] << data[:contract].slice(:local_symbol, :last_trading_day, :con_id) 
            end

          when IB::Messages::Incoming::ContractDataEnd
            sleep 0.25
            client_success(requests[__id__]) { requests[__id__][:result] }
            req_mon.synchronize { requests.delete(__id__) }

          when IB::Messages::Incoming::RealTimeBar
            con_id    = data[:request_id]
            bar       = data[:bar]
            exchange  = persistent[:realtimebars][con_id][:exchange]
            begin
              exchange.publish(bar.slice(*%i[time open high low close volume trades wap]).to_json)
            rescue Bunny::ChannelAlreadyClosed
              ib.send_message :CancelRealTimeBars, id: con_id
              log "Delivery for #{persistent[:realtimebars][con_id][:contract] rescue 'unknown contract' 
                  } with con_id #{con_id} has been stopped." 
              Thread.new{ sleep 5; per_mon.synchronize { persistent[:realtimebars].delete(con_id) } }
            end

          when IB::Messages::Incoming::TickSize,
            IB::Messages::Incoming::TickPrice,
            IB::Messages::Incoming::TickGeneric,
            IB::Messages::Incoming::TickString
            con_id    = data[:ticker_id] 
            contract  = persistent[:ticks][con_id][:contract]
            exchange  = persistent[:ticks][con_id][:exchange]
            begin
              exchange.publish(data.inspect.to_json)
            rescue Bunny::ChannelAlreadyClosed
              ib.send_message :CancelMarketData, id: con_id
              log "Delivery for #{persistent[:ticks][con_id][:contract]} with con_id #{con_id} has been stopped."
              Thread.new{ sleep 0.25; per_mon.synchronize { persistent[:ticks].delete(con_id) } }
            end

          else
            log("WARNING".light_red + "\tUnknown messagetype: #{msg.inspect}")
          end
        end
        log "SPAWN_SUBSCRIBERS\tSubscribers attached to IB" if @debug
      end
      @depth_subscriber_thread = Thread.new do
        loop do
          sleep 0.025 while @block_depth_queue
          msg = @depth_queue.pop
          con_id = msg.data[:request_id] 
          msg[:contract] =  persistent[:depth][con_id][:contract]
          persistent[:depth][con_id][:buffer] << msg.data.slice(*%i[ contract position operation side price size ])
        end
      end
    end
  end
end

