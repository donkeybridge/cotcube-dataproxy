# frozen_string_literal: true

module Cotcube
  # top-level class documentation comment
  class DataProxy

    def initialize(
      outputhandler: OutputHandler.new(
        location: "/var/cotcube/log/dataproxy"
      )
    )
      check_pidfile
      write_pidfile
      @output = outputhandler
      refresh_mq
      refresh_ib
      @requests = {}
      @req_mon  = Monitor.new
      @persistent = { ticks: {}, depth: {}, realtimebars: {} }
      @per_mon  = Monitor.new
      @gc_thread = nil
      spawn_message_subscribers
      commserver_start
      recover
      gc_start
    rescue
      remove_pidfile
      raise
    end

    def refresh_ib
      @client.close rescue nil
      @client = DataProxy.get_ib_client
      p client
      @ib     = @client[:ib]
      raise 'Could not connect to IB' unless @ib
    end

    def refresh_mq
      @mq     = DataProxy.get_mq_client
      raise 'Could not connect to RabbitMQ' if %i[ request_exch replies_exch request_queue ].map{|z| mq[z].nil? }.reduce(:|)
    end

    def shutdown
      log "Shutting down dataproxy."
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
      remove_pidfile
      log "... done."
    end

    def check_pidfile
      raise RuntimeError, "Cannot start up, another instance might be up, please check and possibly remove #{PIDFILE}." if File.exist? PIDFILE
    end

    def write_pidfile
      File.write(PIDFILE, Process.pid)
    end

    private 
    attr_reader :client, :clients, :ib, :mq, :requests, :req_mon, :persistent, :per_mon, :gc_thread

    def recover
      get_mq(list: false)[:exchanges].keys.select{|z| z.split('_').size == 3 }.each do |exch|
        src, type, contract = exch.split('_')
        next unless src == 'dataproxy'
        next unless %w[ ticks depth realtimebars ].include? type.downcase
        log "Found #{exch} to recover."
        subscribe_persistent( { contract: contract, exchange: exch }, type: type.to_sym )
      end
    end

    def log(msg)
      @output.puts "#{DateTime.now.strftime('%Y%m%d-%H:%M:%S:  ')}#{msg.to_s.scan(/.{1,120}/).join("\n" + ' ' * 20)}"
    end

    def remove_pidfile
      File.delete(PIDFILE) if File.exist? PIDFILE
    end

  end
end

__END__
