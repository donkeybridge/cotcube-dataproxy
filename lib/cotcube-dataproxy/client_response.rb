module Cotcube
  class DataProxy

    def client_success(request, id: nil, to: nil, exchange: :replies_exch, &block)
      client_response( request, id: nil, to: nil, err: 0, exchange: exchange, &block)
    end

    def client_fail(request,    id: nil, to: nil, err: 1,exchange: :replies_exch, &block)
      client_response(request,  id: nil, to: nil, err: err, exchange: exchange, &block)
    end

    private

    def client_response(request, id: nil, to: nil, err:, exchange: :replies_exch)
      __id__ = id.presence || request.delete(:__id__)
      __to__ = to.presence || request.delete(:__to__)
      msg = yield
      case msg
      when String
        response = { error: err, msg: msg }
      when Hash
        response = { error: err }
        msg.each { |k, v| response[k] = v }
      when Array
        response = { error: err, result: msg }
      else
        response = { error: 1, msg: "Processing failed for '#{msg.inspect}' after '#{request}'." }
      end
      if response[:error].positive?
        log "CLIENT #{__id__} FAILURE:      #{response.inspect}.".colorize(:light_red)
      elsif response[:result].is_a?(Array)
        log "CLIENT #{__id__} SUCCESS:       sent #{response[:result].size} datasets."
      else
        log "CLIENT #{__id__} SUCCESS:       #{response.to_s[..220].scan(/.{1,120}/).join(' '*30 + "\n")}"
      end
      mq[exchange].publish(
        response.to_json,
        content_type:    'application/json',
        priority: 7,
        correlation_id:  __id__,
        routing_key:     __to__,
        reply_to:        __id__
      )
    end
  end
end
