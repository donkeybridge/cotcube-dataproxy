module Cotcube
  class DataProxy

    # Create a connection to the locally running
    def self.get_ib_client(host: 'localhost', port: 4002, id: 5, client_id: 5)
      obj = {
        id: id,
        client_id: client_id,
        port: port,
        host: host
      }
      begin
        obj[:ib] = IB::Connection.new(
          id: id,
          client_id: client_id,
          port: port,
          host: host
        ) do |provider|
          obj[:alert]            = provider.subscribe(:Alert) { true }
          obj[:managed_accounts] = provider.subscribe(:ManagedAccounts) { true }
        end
        obj[:error] = 0
      rescue Exception => e
        obj[:error] = 1
        obj[:message] = e.message
        obj[:full_message] = e.full_message
      end
      obj
    end


    def self.get_mq_client(client_id: 5)
      obj = {
        client_id: client_id,
      }
      begin
        # for more info on connection parameters see http://rubybunny.info/articles/connecting.html
        #
        obj[:connection]    = Bunny.new(
          host: 'localhost',
          port: 5672,
          user: SECRETS['dataproxy_mq_user'],
          password: SECRETS['dataproxy_mq_password'],
          vhost: SECRETS['dataproxy_mq_vhost']
        )
        obj[:connection].start
        obj[:commands]      = obj[:connection].create_channel
        obj[:channel]       = obj[:connection].create_channel
        obj[:request_queue] = obj[:commands].queue('', exclusive: true, auto_delete: true)
        obj[:request_exch]  = obj[:commands].direct('dataproxy_commands', exclusive: true, auto_delete: true)
        obj[:replies_exch]  = obj[:commands].direct('dataproxy_replies', auto_delete: true)
        %w[ dataproxy_commands  ].each do |key|
          obj[:request_queue].bind(obj[:request_exch], routing_key: key )
        end
        obj[:error]      = 0
      rescue Exception => e
        obj[:error] = 1
        obj[:message] = e.message
        obj[:full_message] = e.full_message
      end
      obj
    end

  end
end
