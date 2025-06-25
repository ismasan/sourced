# frozen_string_literal: true

module ExecutorExamples
  RSpec.shared_examples 'an executor' do
    it 'runs work concurrently' do
      results = []
      queue = Thread::Queue.new
      executor.start do |task|
        task.spawn do
          sleep 0.00001
          queue << 1
        end

        task.spawn do
          queue << 2
        end
      end

      queue.close
      while (it = queue.pop)
        results << it
      end

      expect(results).to eq([2, 1])
    end

    it 'waits and re-raises errors' do
      expect do
        executor.start do |task|
          task.spawn do
            raise ArgumentError, 'Test error'
          end

          task.spawn do
            
          end
        end
      end.to raise_error(ArgumentError, 'Test error')
    end
  end
end
