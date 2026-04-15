require_relative 'app'

require 'sourced/ui/dashboard'
require 'datastar/async_executor'
Datastar.config.executor = Datastar::AsyncExecutor.new

Sourced::UI::Dashboard.configure do |config|
  config.header_links([
    { label: 'back to app', href: '/', url: false }
  ])
end

map '/sourced' do
  run Sourced::UI::Dashboard
end

map '/' do
  run App
end
