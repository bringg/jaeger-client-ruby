require 'spec_helper'

describe Jaeger::Client::Collector do
  let(:collector) { described_class.new }
  let(:sender) { spy }
  let(:default_tracer) { Jaeger::Client::Tracer.new(collector, sender) }
  let(:operation_name) { 'op-name' }

  context '#send_span' do
    let(:span) { default_tracer.start_span(operation_name) }

    it 'should buffer debug spans' do
      span.context.debug = true
      span.context.sampled = false
      collector.send_span(span, Time.now)
      expect(collector.retrieve).to_not be_empty
    end

    it 'should buffer sampled spans' do
      span.context.debug = true
      span.context.sampled = false
      collector.send_span(span, Time.now)
      expect(collector.retrieve).to_not be_empty
    end

    it 'should not buffer non-sampled, non-debug spans' do
      span.context.debug = false
      span.context.sampled = false
      collector.send_span(span, Time.now)
      expect(collector.retrieve).to be_empty
    end
  end
end
