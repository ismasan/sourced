module Sourced
  module Persistable
    def self.included(base)
      base.extend ClassMethods
    end

    def persist
      self.class.repository.persist(self)
    end

    module ClassMethods
      def repository(repo = nil)
        @repository = repo if repo
        @repository || Sourced.configuration.aggregate_repo
      end

      def load(id)
        repository.load(id, self)
      end

      def build
        repository.build(self)
      end
    end
  end
end
