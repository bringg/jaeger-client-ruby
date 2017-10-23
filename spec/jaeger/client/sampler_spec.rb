require 'spec_helper'

RSpec.describe 'Samplers' do
  describe 'Sampler' do
    let(:const_setup) { { type: 'const', param: true } }
    let(:ratelimiting_setup) { { type: 'ratelimiting', param: 1.0 } }
    let(:probabilistic_setup) { { type: 'probabilistic', param: 1.0 } }
    let(:unknown_setup) { { type: 'no-such-sampler', param: 1.0 } }

    it 'defaults to ConstSampler with true decision' do
      sampler = Jaeger::Client::Sampler.build
      expect(sampler)
        .to be_instance_of(Jaeger::Client::ConstSampler)
      expect(sampler.tags['sampler.param']).to eq(true)
    end

    it 'builds ConstSampler' do
      expect(Jaeger::Client::Sampler.build(const_setup))
        .to be_instance_of(Jaeger::Client::ConstSampler)
    end

    it 'builds ProbabilisticSampler' do
      expect(Jaeger::Client::Sampler.build(probabilistic_setup))
        .to be_instance_of(Jaeger::Client::ProbabilisticSampler)
    end

    it 'builds RatelimitingSampler' do
      expect(Jaeger::Client::Sampler.build(ratelimiting_setup))
        .to be_instance_of(Jaeger::Client::RatelimitingSampler)
    end

    it 'fails with unknown setup' do
      expect(-> { Jaeger::Client::Sampler.build(unknown_setup) })
        .to raise_error(RuntimeError, "Unknown sampler type 'no-such-sampler'")
    end
  end

  describe 'ConstSampler' do
    context 'when true sampler' do
      let(:sampler) { Jaeger::Client::ConstSampler.new }

      it 'samples every span' do
        expect(sampler)
          .to be_is_sampled(Jaeger::Client::TraceId.generate, 'test')
      end
    end

    context 'when false sampler' do
      let(:sampler) { Jaeger::Client::ConstSampler.new(false) }

      it 'samples no spans' do
        expect(sampler)
          .not_to be_is_sampled(Jaeger::Client::TraceId.generate, 'test')
      end
    end

    it 'provides sampler tags' do
      sampler = Jaeger::Client::ConstSampler.new
      expect(sampler.tags['sampler.type']).to eq('const')
      expect(sampler.tags['sampler.param']).to eq(true)
    end
  end

  describe 'ProbabilisticSampler' do
    let(:sampler) { Jaeger::Client::ProbabilisticSampler.new(0.5) }

    it 'samples traceid under boundary' do
      trace_id = (0.45 * (2**63 - 1)).to_i
      expect(sampler).to be_is_sampled(trace_id, 'test')
    end

    it 'does not sample traceid over boundary' do
      trace_id = (0.55 * (2**63 - 1)).to_i
      expect(sampler).not_to be_is_sampled(trace_id, 'test')
    end

    it 'provides sampler tags' do
      expect(sampler.tags['sampler.type']).to eq('probabilistic')
      expect(sampler.tags['sampler.param']).to eq(0.5)
    end
  end

  describe 'RatelimitingSampler' do
    let(:rate) { 5.0 }
    let(:sampler) { Jaeger::Client::RatelimitingSampler.new(rate) }
    let(:trace_id) { Jaeger::Client::TraceId.generate }

    it 'samples traceid under boundary' do
      expect(sampler).to be_is_sampled(trace_id, 'test')
    end

    it 'does not sample traceid when no credits' do
      6.times { sampler.is_sampled?(trace_id, 'test') }
      expect(sampler).not_to be_is_sampled(trace_id, 'test')
    end

    it 'accumulates balance' do
      sampler.is_sampled?(trace_id, 'test')
      allow(Time).to receive(:now).and_return(Time.now + 60)
      expect(sampler).to be_is_sampled(trace_id, 'test')
      expect(sampler).to be_is_sampled(trace_id, 'test')
    end

    it 'has sampler tags' do
      expect(sampler.tags['sampler.type']).to eq('ratelimiting')
      expect(sampler.tags['sampler.param']).to eq(rate)
    end
  end
end
