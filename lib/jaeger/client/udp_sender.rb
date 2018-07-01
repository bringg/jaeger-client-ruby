# frozen_string_literal: true

require_relative './udp_sender/transport'
require 'socket'
require 'thread'

module Jaeger
  module Client
    class UdpSender
      def initialize(service_name:, host:, port:, collector:, flush_interval:
                     #, flush_span_limit:
      )
        @service_name = service_name
        @collector = collector
        @flush_interval = flush_interval
        #@flush_span_limit = flush_span_limit

        @tags = [
          Jaeger::Thrift::Tag.new(
            'key' => 'jaeger.version',
            'vType' => Jaeger::Thrift::TagType::STRING,
            'vStr' => 'Ruby-' + Jaeger::Client::VERSION
          ),
          Jaeger::Thrift::Tag.new(
            'key' => 'hostname',
            'vType' => Jaeger::Thrift::TagType::STRING,
            'vStr' => Socket.gethostname
          )
        ]
        ipv4 = Socket.ip_address_list.find { |ai| ai.ipv4? && !ai.ipv4_loopback? }
        unless ipv4.nil?
          @tags << Jaeger::Thrift::Tag.new(
            'key' => 'ip',
            'vType' => Jaeger::Thrift::TagType::STRING,
            'vStr' => ipv4.ip_address
          )
        end

        transport = Transport.new(host, port)
        protocol = ::Thrift::CompactProtocol.new(transport)
        @client = Jaeger::Thrift::Agent::Client.new(protocol)
      end

      def start
        # Sending spans in a separate thread to avoid blocking the main thread.
        @thread = Thread.new do
          loop do
            log("ThriftSender: Start @flush_interval: #{@flush_interval}, sleep: #{@flush_interval}, object_id: #{@collector.object_id}, length: #{@collector.length}")
            emit_batch(@collector.retrieve)
            sleep @flush_interval
          end
        end

        #@thread = Thread.new do
        #  loop do
        #    loop do
        #      data = @collector.retrieve(@flush_span_limit)
        #      break if !data.length <= 0
        #      emit_batch(data)
        #    end
        #    sleep @flush_interval
        #  end
        #end
      end

      def stop
        @thread.terminate if @thread
        emit_batch(@collector.retrieve)
      end

      private
      def log(msg)
        Rails.logger.error(msg) if Rails && Rails.logger.present?
      end

      def emit_batch(thrift_spans)
        if thrift_spans.empty?
          log("ThriftSender: emit_batch empty")
          return
        end

        log("ThriftSender: emit_batch sending")
        batch = Jaeger::Thrift::Batch.new(
          'process' => Jaeger::Thrift::Process.new(
            'serviceName' => @service_name,
            'tags' => @tags
          ),
          'spans' => thrift_spans
        )

        @client.emitBatch(batch)
      end
    end
  end
end
