# frozen_string_literal: true

module Cotcube
  class DataProxy

    def commserver_start
      mq[:request_subscription] = mq[:request_queue].subscribe do |delivery_info, properties, payload|

        ################################################################################################
        # the request will be JSON decoded. The generic command 'failed' will be set, if decoding raises.
        # furthermore, __id__ and __to__ are extracted and added to the request-hash
        #
        request    = JSON.parse(payload, symbolize_names: true) rescue { command: 'failed' }
        request[:command] ||= 'nil'

        request[:__id__] = properties[:correlation_id]
        request[:__to__] = properties[:reply_to]

        if request[:debug] 
          log "Received \t#{delivery_info.map{|k,v| "#{k}\t#{v}"}.join("\n")
                     }\n\n#{properties   .map{|k,v| "#{k}\t#{v}"}.join("\n")
                     }\n\n#{request      .map{|k,v| "#{k}\t#{v}"}.join("\n")}" if request[:debug]
        else
          log "Received\t#{request}"
        end

        ###############################################################################################
        # the entire set of command processing,
        # starting with the (generic) 'failed' command, that just answers with the failure notice
        # and      with another failure notice upon a missing command section in the request
        # ending   with another failure notice, if an unknown command was issued
        #
        log "Processing #{request[:command]}:"
        case request[:command].downcase
        when 'failed'
          client_fail(request) { "Failed to parse payload: '#{payload}'." }

        when 'nil'
          client_fail(request) { "missing :command in request: '#{request}'." }

        ##############################################################################################
        # ping -> pong, just for testing
        # 
        when 'ping'
          client_success(request) { "pong" }

        ##############################################################################################
        # the get_contracts command tries to resolve a list of available contracts related to a
        #     specific symbol based on a set of characteristics retrieved via Herlpers::get_id_set
        #
        # the reply of the message is processed asynchroniously upon reception of
        #     the IB Message 'ContractDataEnd' in the message subscribers section
        # 
        when 'get_contract', 'get_contracts', Cotcube::Helpers.sub(minimum: 3) { 'contracts' }
          if request[:symbol].nil? 
            client_fail(request) { "Cannot requets contracts without :symbol (in '#{request}')." }
            next
          end
          sym = Cotcube::Helpers.get_id_set(symbol: request[:symbol])
          if [nil, false].include? sym
            client_fail(request) { "Unknown symbol '#{request[:symbol]}' in '#{request}'." }
            next
          end
          request[:result] = [] 
          req_mon.synchronize { requests[request[:__id__]] = request }
          ib_contract = IB::Contract.new symbol: sym[:ib_symbol], exchange: sym[:exchange], currency: sym[:currency], sec_type: (request[:sec_type] || 'FUT') 
          ib.send_message :RequestContractData, contract: ib_contract, request_id: request[:__id__].to_i(16)


        ##############################################################################################
        # the historical command retrieves a list of bars as provided by TWS
        # the minimum requirement is :contract parameter issued.
        # 
        # the reply to this message is processed asynchroniously upon reception of
        #     the IB message 'HistoricalData' in message subscribers section
        #
        when Cotcube::Helpers.sub(minimum: 3) {'historical'}
          con_id = request[:con_id] || Cotcube::Helpers.get_ib_contract(request[:contract])[:con_id] rescue nil
          if con_id.nil? or request[:contract].nil? 
             client_fail(request) { "Cannot get :con_id for contract:'#{request[:contract]}' in '#{request}'." } 
             next
          end
          sym    = Cotcube::Helpers.get_id_set(contract: request[:contract])
          before = Time.at(request[:before]).to_ib rescue Time.now.to_ib
          ib_contract = IB::Contract.new(con_id: con_id, exchange: sym[:exchange])
          req = {
            request_id:    request[:__id__].to_i(16),
            contract:      ib_contract,
            end_date_time: before,
            what_to_show:  (request[:based_on]                || :trades),
            use_rth:       (request[:rth_only]                     || 1),
            keep_up_to_date: 0,
            duration:      (request[:duration].gsub('_', ' ') || '1 D'),
            bar_size:      (request[:interval].to_sym         || :min15)
          }
          req_mon.synchronize { requests[request[:__id__]] = request }
          begin
            Timeout.timeout(2) { ib.send_message(IB::Messages::Outgoing::RequestHistoricalData.new(req)) }
          rescue Timeout::Error, IB::Error
            client_fail(request) { 'Could not request historical data. Is ib_client running?' }
            req_mon.synchronize { requests.delete(request[:__id__]) } 
            next
          end


        # ********************************************************************************************
        #
        # REQUESTS BELOW ARE BASED ON A cONTINUOUS IB SUBSCRIPTION AND MUST BE CONSIDERED
        # GARBAGE COLLECTION ( instance.gc ) --- SUBSCRIPTION DATA IS PERSISTET IN @persistent
        #
        # ********************************************************************************************


        ###############################################################################################
        # the start_realtimebars initiates the IBKR realtime (5s) bars delivery for a specific contract
        #     and feeds them into a fanout exchange dedicated to that contract
        #     delivery continues as long as there are queues bound to that exchange
        # 
        # obviously the first requestor in itiates, the latter ones just attach their queue to this exchange
        when Cotcube::Helpers.sub(minimum:4){'realtimebars'}
          subscribe_persistent(request, type: :realtimebars)
          next
          sym    = Cotcube::Helpers.get_id_set(contract: request[:contract])
          con_id = request[:con_id] || Cotcube::Helpers.get_ib_contract(request[:contract])[:con_id] rescue nil
          if sym.nil? or con_id.nil?
            client_fail(request) { "Invalid contract '#{request[:contract]}'." } 
            next
          end
          if persistent[:realtimebars][con_id].nil?
            per_mon.synchronize { 
              persistent[:realtimebars][con_id] = { con_id: con_id, 
                                                    contract: request[:contract], 
                                                    exchange: mq[:channel].fanout(request[:exchange]) } 
            } 
            ib_contract = IB::Contract.new(con_id: con_id, exchange: sym[:exchange])
            ib.send_message(:RequestRealTimeBars, id: con_id, contract: ib_contract, data_type: :trades, use_rth: false)
            client_success(request) { "Delivery of realtimebars of #{request[:contract]} started." }
          elsif persistent[:realtimebars][con_id][:on_cancel] 
            client_fail(request) { { reason: :on_cancel, message: "Exchange '#{requst[:exchange]}' is marked for cancel, retry in a few seconds to recreate" } }
            next
          else
            client_success(request) { "Delivery of realtimebars of #{request[:contract]} attached to existing." } 
          end

        when 'ticks'
          subscribe_persistent(request, type: :realtimebars)
          next
          sym    = Cotcube::Helpers.get_id_set(contract: request[:contract])
          con_id = request[:con_id] || Cotcube::Helpers.get_ib_contract(request[:contract])[:con_id] rescue nil
          if sym.nil? or con_id.nil?
            client_fail(request) { "Invalid contract '#{request[:contract]}'." }
            next
          end
          if persistent[:ticks][con_id].nil?
            per_mon.synchronize { 
              persistent[:ticks][con_id] = { con_id: con_id,
                                             contract: request[:contract],
                                             exchange: mq[:channel].fanout(request[:exchange]) } 
            }
            ib_contract = IB::Contract.new(con_id: con_id, exchange: sym[:exchange])
            ib.send_message(:RequestMarketData, id: con_id, contract: ib_contract)
          elsif persistent[:ticks][con_id][:on_cancel]
            client_fail(request) { { reason: :on_cancel, message: "Exchange '#{requst[:exchange]}' is marked for cancel, retry in a few seconds to recreate" } }
            next
          end
          client_success(request) { "Delivery of ticks of #{request[:contract]} started." }

        when 'depth'
          subscribe_persistent(request, type: :depth)
          next
          sym    = Cotcube::Helpers.get_id_set(contract: request[:contract])
          con_id = request[:con_id] || Cotcube::Helpers.get_ib_contract(request[:contract])[:con_id] rescue nil
          if sym.nil? or con_id.nil? 
            client_fail(request) { "Invalid contract '#{request[:contract]}'." }
            next
          end
          if persistent[:depth][con_id].nil?
            per_mon.synchronize { 
              persistent[:depth][con_id] = { con_id: con_id, contract: request[:contract],
                                             exchange: mq[:channel].fanout(request[:exchange]), 
                                             buffer: [] } 
            }
            bufferthread = Thread.new do
              sleep 5.0 - (Time.now.to_f % 5)
              loop do
                begin
                  # TODO: This is very basic mutual exclusion !!! 
                  @block_depth_queue = true
                  sleep 0.025
                  con = persistent[:depth][con_id]
                  con[:exchange].publish(con[:buffer].to_json)
                  con[:buffer] = [] 
                  @block_depth_queue = false
                end
                sleep 5.0 - (Time.now.to_f % 5)
              end
            end
            per_mon.synchronize { persistent[:depth][con_id][:bufferthread] = bufferthread }
            ib_contract = IB::Contract.new(con_id: con_id, exchange: sym[:exchange])
            ib.send_message(:RequestMarketDepth, id: con_id, contract: ib_contract, num_rows: 10) 
          elsif persistent[:ticks][con_id][:on_cancel]
            client_fail(request) { { reason: :on_cancel, message: "Exchange '#{requst[:exchange]}' is marked for cancel, retry in a few seconds to recreate" } }
            next
          end
          client_success(request) { "Delivery of market depth of #{request[:contract]} started." }

        else
          client_fail(request) { "Unknown :command '#{request[:command]}' in '#{request}'." }
        end
      end
      log "Started commserver listening on #{mq[:request_queue]}"
    end

    def commserver_stop
      mq[:request_subscription].cancel
      log "Stopped commserver ..."
    end

    def subscribe_persistent(request, type:)
      sym    = Cotcube::Helpers.get_id_set(contract: request[:contract])
      con_id = request[:con_id] || Cotcube::Helpers.get_ib_contract(request[:contract])[:con_id] rescue nil
      if sym.nil? or con_id.nil?
        client_fail(request) { "Invalid contract '#{request[:contract]}'." }
        return
      end
      if persistent[type][con_id].nil?
        per_mon.synchronize {
          persistent[type][con_id] = { con_id: con_id,
                                       contract: request[:contract],
                                       exchange: mq[:channel].fanout(request[:exchange]) }
        }
        if type == :depth
          bufferthread = Thread.new do
            sleep 5.0 - (Time.now.to_f % 5)
            loop do
              begin
                @block_depth_queue = true
                sleep 0.025
                con = persistent[:depth][con_id]
                con[:exchange].publish(con[:buffer].to_json)
                con[:buffer] = []
                @block_depth_queue = false
              end
              sleep 5.0 - (Time.now.to_f % 5)
            end
          end
          per_mon.synchronize { persistent[:depth][con_id][:bufferthread] = bufferthread }
        end
        ib_contract = IB::Contract.new(con_id: con_id, exchange: sym[:exchange])
        ib.send_message(REQUEST_TYPES[type], id: con_id, contract: ib_contract, data_type: :trades, use_rth: false)
        client_success(request) { "Delivery of #{type} of #{request[:contract]} started." }
      elsif persistent[type][con_id][:on_cancel]
        client_fail(request) { { reason: :on_cancel, message: "Exchange '#{requst[:exchange]}' is marked for cancel, retry in a few seconds to recreate" } }
      else
        client_success(request) { "Delivery of #{type} of #{request[:contract]} attached to existing." }
      end
    end


  end
end
