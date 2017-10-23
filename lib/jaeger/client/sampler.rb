module Jaeger
  module Client

    class SamplerTags
      def self.build(type, param)
        { 'sampler.type' => type,
          'sampler.param' => param }
      end
    end

    class Sampler
      attr_reader :tags

      # Build a sampler base on client build params, the names and params try
      # to reflect implementation from other language libraries to provide similar
      # options for configuring sampler.
      #
      # @param type [String] Sampler type
      # @param param [Float] Sampler specific param, see sampler implementations
      #
      # @return [Jaeger::Client::Sampler] Sampler
      #
      # @example
      #   Jaeger::Client::Sampler.build(type: 'probabilistic', param: 0.001)
      #          #=> Jaeger::Client::ProbabilisticSampler
      def self.build(type: 'const', param: true)
        case type.to_s
        when 'const'
          ConstSampler.new(param)
        when 'probabilistic'
          ProbabilisticSampler.new(param)
        when 'ratelimiting'
          RatelimitingSampler.new(param)
        else
          raise "Unknown sampler type '#{type}'"
        end
      end

      # The canonical name for the sampler, generated from the class name
      #
      # @return [String] sampler name
      def name
        self.class.name.split('::').last.sub('Sampler', '').downcase
      end

      # Return tracing decision
      #
      # @param trace_id [TraceId] the context of the span
      # @param operation_name [String] the operation name
      #
      # @return [Boolean] the decicion, wether to sample or not
      def is_sampled?(trace_id, operation_name)
        make_decision(trace_id, operation_name)
      end
    end

    # ConstSampler
    #
    # Either always sample, or never.
    #
    #
    class ConstSampler < Sampler
      def initialize(decision = true)
        @decision = decision
        @tags = SamplerTags.build(name, decision)
      end

      def make_decision(_trace_id, _operation_name)
        @decision
      end
    end

    # ProbabilisticSampler
    #
    # Sample a portion of traces using trace_id as the random decision
    class ProbabilisticSampler < Sampler
      def initialize(rate = 0.001)
        rate = rate.to_f
        if rate < 0.0 || rate > 1.0
          raise "samplingrate must be  0.0...1.0, received #{rate}"
        end
        @sampler_param = rate
        @samplingboundary = Jaeger::Client::TraceId::TRACE_ID_UPPER_BOUND *
                            rate
        @tags = SamplerTags.build(name, rate)
      end

      def make_decision(trace_id, _operation_name)
        (@samplingboundary >= trace_id)
      end
    end

    # RatelimitingSampler
    #
    # Sample a configured amount of traces per second. Uses a credit balance,
    # incremented by an amount proportional to elapsed time since last check. If
    # balance allows, sampler will return positive decision and deduct operation
    # cost from balance (cost is always 1.0 ).
    # Accumulated credit is limited to amount of traces per second.
    #
    class RatelimitingSampler < Sampler
      attr_reader :balance, :last_tick

      def initialize(max_traces_per_second = 1.0)
        @max_traces_per_second = max_traces_per_second
        @tags = SamplerTags.build(name, max_traces_per_second)
        @balance = 1.0
        @max_balance = [max_traces_per_second, 1.0].max
        @last_tick = Time.now
        @mutex = Mutex.new
      end

      def make_decision(_trace_id, _operation_name)
        @mutex.synchronize do
          calculate_balance
          if @balance >= 1.0
            @balance -= 1.0
            return true
          end
          return false
        end
      end

      # Increment balance proportional to elapsed time since last decision.
      # Limit balance to traces-per-second.
      def calculate_balance
        current_time = Time.now
        @balance += ((current_time - @last_tick) * @max_traces_per_second)
        @balance = [@balance, @max_balance].min
        @last_tick = current_time
      end
    end
  end
end
