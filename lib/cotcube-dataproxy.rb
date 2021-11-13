#!/usr/bin/env ruby

require 'bundler'
Bundler.require
require_relative './cotcube-dataproxy/3rd_clients'
require_relative './cotcube-dataproxy/init'
require_relative './cotcube-dataproxy/client_response'
require_relative './cotcube-dataproxy/subscribers'
require_relative './cotcube-dataproxy/commserver'
require_relative './cotcube-dataproxy/gc'

SECRETS_DEFAULT = { 
  'dataproxy_mq_proto'    => 'http',
  'dataproxy_mq_user'     => 'guest', 
  'dataproxy_mq_password' => 'guest',
  'dataproxy_mq_host'     => 'localhost',
  'dataproxy_mq_port'     => '15672',
  'dataproxy_mq_vhost'    => '%2F'
}

# Load a yaml file containing actual parameter and merge those with current
# TODO use better config file location
SECRETS = SECRETS_DEFAULT.merge( -> {YAML.load(File.read("#{__FILE__.split('/')[..-2].join('/')}/../secrets.yml")) rescue {} }.call)

CANCEL_TYPES = {
  ticks: :CancelMarketData,
  depth: :CancelMarketDepth,
  realtimebars: :CancelRealTimeBars
}

REQUEST_TYPES = { 
  ticks: :RequestMarketData,
  depth: :RequestMarketDepth,
  realtimebars: :RequestRealTimeBars
}

