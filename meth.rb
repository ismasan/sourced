class Foo
  class << self
    def command(sym)
      raise self.method(sym).inspect
    end
  end

  command def foo(name:); end
end
