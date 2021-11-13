# frozen_string_literal: true

module Cotcube
  # top-level class documentation comment
  class DataProxy

    def initialize(
      outputhandler: OutputHandler.new(
        location: "/var/cotcube/log/dataproxy"
      ),
    )
      @output = outputhandler
      @client = DataProxy.get_ib_client
      @mq     = DataProxy.get_mq_client
      @ib     = @client[:ib]
      raise 'Could not connect to IB' unless @ib
      raise 'Could not connect to RabbitMQ' if %i[ request_exch replies_exch request_queue ].map{|z| mq[z].nil? }.reduce(:|)
      @requests = {}
      @req_mon  = Monitor.new
      @persistent = { ticks: {}, depth: {}, realtimebars: {} }
      @per_mon  = Monitor.new
      @gc_thread = nil
      spawn_message_subscribers
      commserver_start
      recover
      gc_start
    end

    def shutdown
      puts "Shutting down dataproxy."
      commserver_stop
      gc_stop
      mq[:commands].close
      mq[:channel].close
      mq[:connection].close
      persistent.each do |type, items|
        items.each do |con_id, item|
          log "sending #{ CANCEL_TYPES[type.to_sym]} #{con_id} (for #{item[:contract]})"
          ib.send_message CANCEL_TYPES[type.to_sym], id: con_id
        end
      end
      sleep 1
      gc
      sleep 1
      ib.close
      puts "... done."
    end

    private 
    attr_reader :client, :clients, :ib, :mq, :requests, :req_mon, :persistent, :per_mon, :gc_thread

    def recover
      get_mq(list: false)[:exchanges].keys.select{|z| z.split('_').size == 3 }.each do |exch|
        src, type, contract = exch.split('_')
        next unless src == 'dataproxy'
        next unless %w[ ticks depth realtimebars ].include? type.downcase
        puts "Found #{exch} to recover."
        subscribe_persistent( { contract: contract, exchange: exch }, type: type.to_sym )
      end
    end

    def log(msg)
      @output.puts "#{DateTime.now.strftime('%Y%m%d-%H:%M:%S:  ')}#{msg.to_s.scan(/.{1,120}/).join("\n" + ' ' * 20)}"
    end

  end
end

__END__
