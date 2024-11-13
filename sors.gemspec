# frozen_string_literal: true

require_relative 'lib/sors/version'

Gem::Specification.new do |spec|
  spec.name = 'sors'
  spec.version = Sors::VERSION
  spec.authors = ['Ismael Celis']
  spec.email = ['ismaelct@gmail.com']

  spec.homepage = 'https://github.com/ismaelcelis/sors'
  spec.summary = 'Decide, Evolve, React'
  spec.required_ruby_version = '>= 3.0.0'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir = 'exe'
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'async'
  spec.add_dependency 'plumb', '>= 0.0.8'

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
