## 0.1.2 (November 28, 2021)
  - subscribers/historical: returning improved values hash
  - added validator for contract to sub historical
  - client_response: improvied log output
  - 3rd-clients: removed auto-delete (due to working GC)

## 0.1.1 (November 13, 2021)
  - added handy test_script in bin/test_client.rb
  - added httparty to Gem requirements
  - added handy startup script to bin/dataproxy
  - removed superflouus lines in commserver.rb'
  - gc.rb: the garbage collector. containing gc, gc_start/_stop, api() (uses management api to get a list), get_mq ( for listings) and delete_mq (for API based deletion)
  - commserver.rb, handling incoming commands from clients. containing commserver_start/_stop and subscribe_persistent
  - subscribers.rb, handling of incoming IB messages. PLUS __int2hex__, just a tiny helper to translate IDs
  - client_response.rb, containing client_success and client_fail
  - init.rb: containing initialize, shutdown, recover and log
  - 3rd_clients: class level helpers to create IB and RabbitMQ client objects
  - central startup file, containing bundler, requires and constants
  - added startup script
  - initial commit. adding Gemfile and gemspec

## 0.1.0 (November 10, 2021)


