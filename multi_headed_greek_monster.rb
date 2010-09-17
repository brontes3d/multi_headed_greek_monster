class MultiHeadedGreekMonster
  
  def initialize(progress = nil, worker_count = 3, on_port = 23121, &block)
    @action = block
    @on_port = on_port
    @worker_count = worker_count
    @progress = progress
    start_service
    start_workers
  end
  
  def feed(thing)
    @service_manager.give(thing)
  end
  
  def wait(for_min_q_size = 5, &block)
    while(@service_manager.q_size > for_min_q_size)
      sleep(1)
      if block_given?
        yield
      end
    end
  end
  
  def finish
    @service_manager.done!
    while(!@service_manager.done?)
      sleep(1)
    end
    @worker_pids.each do |pid|
      Process.wait(pid)      
    end
    Process.kill("KILL", @server_pid)
  end
  
  class ServiceManager
    def initialize(progress)
      @progress = progress
      @things = []
      @done = false
    end
    def give(thing)
      @things << thing
    end
    def take
      @things && @things.pop
    end
    def q_size
      @things && @things.size || 0
    end
    def done?
      @done && @things && @things.empty?
    end
    def done!
      @done = true
    end
    def tick
      @progress.tick if @progress
    end
  end
  
  private
  
  def start_service
    require 'drb'
    
    @server_pid = fork do
      ActiveRecord::Base.clear_all_connections!
      at_exit { exit! }
      DRb.start_service "druby://localhost:#{@on_port}", ServiceManager.new(@progress)
      DRb.thread.join
    end

    @service_manager = DRbObject.new nil, "druby://localhost:#{@on_port}"
    sleep 0.2 # FIXME
  end
  
  def start_workers
    @worker_pids = []
    @worker_count.times do |i|
      @worker_pids << fork do
        ActiveRecord::Base.clear_all_connections!
        sleep(i)
        at_exit { exit! }
        work = DRbObject.new nil, "druby://localhost:#{@on_port}"
        waits = 0
        while(!work.done?)
          if got = work.take
            @action.call(got, work)
            waits = 0
          else
            if waits > 10
              puts "waiting on work to do from #{Process.pid} (#{waits})"
            end
            sleep(1)
            waits += 1
          end
        end
      end
    end
  end
  
end