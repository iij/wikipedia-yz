#!/bin/env ruby
#
# wikipedia-yz.rb
#
#

require 'msgpack/rpc'
require 'optparse'
require 'riak'

PATH_HOSTS = "hosts.rb"
if File.exist? PATH_HOSTS
  load PATH_HOSTS
end

NUM_WORKER = 8
RPC_PORT=32768
HOSTS ||= [{:host => "127.0.0.1", :pb_port => 8087}]

class WikiDump
  def open(filename, arg2)
    @fp = File.open(filename)
    @fn = filename
    if arg2 != nil
      @chunk_size = @fp.size / arg2
      @range_set = range_calculate(@chunk_size)
    # p @range_set
    else
      @chunk_size = @fp.size
      @range_set = [[0, @chunk_size-1]]
    end
  end

  def initialize(arg1, arg2=nil)
    open(arg1, arg2)
  end

  def close
    @fp.close
  end

  def dup
    @fp.close
    @fp = File.open(@fn)
  end

  def seek(idx)
    @fp.seek(@range_set[idx][0], IO::SEEK_SET)
  end

  def pos
    @fp.pos
  end

  def range_end(idx)
    @range_set[idx][1]
  end

  def nworker
    @range_set.size
  end

  def range_calculate(chunk_size)
    range_ary = []
    pos_start = 0
    while pos_start < @fp.size
      @fp.seek(pos_start)
      @fp.gets # skip to next line
      pos_start = @fp.pos
      while line = @fp.gets
        if line =~ /\<page\>/
          pos_start = @fp.pos - line.size
          break
        end
      end
      pos_end = pos_start + chunk_size
      @fp.seek(pos_end)
      @fp.gets # XXX skip to next line
      pos_end = @fp.pos
      while line = @fp.gets
        if line =~ /\<\/page\>/
          pos_end = @fp.pos
          break
        end
      end
      range_ary.push([pos_start, pos_end])
      pos_start = pos_end + 1
    end
    @fp.rewind
    range_ary
  end

  def slice
    parse_state = nil
    out_str = ""
    while line = @fp.gets
      if parse_state == :page
        if line =~ /\<\/page\>/
          return out_str + line
        end
        out_str += line
      end
      if line =~ /\<page\>/
        parse_state = :page
        out_str = line
        next
      end
    end
  end
end

class StatCollect
  def initialize
    @num_objects = 0
    @nreport = 0
  end

  def report(numobject)
    @num_objects += numobject
    @nreport += 1
  end

  def stats
    @num_objects
  end
end

class RiakUpload
  def initialize(num_server = 1)
    @rc = Riak::Client.new(:nodes => HOSTS[0...num_server])
  end

  def obj_put(key, value)
    bucket = @rc.bucket("wikipedia")
    #bucket.props = {:n_val => 1, :dw => 0}
    obj = Riak::RObject.new(bucket, key)
    #obj.content_type = "text/xml"
    obj.content_type = "text/plain"
    obj.raw_data = value
    begin
      obj.store
    rescue
      return nil
    end
    return true
  end
end

OPTS = {}
opt = OptionParser.new
opt.on('-p [VAL]') {|v| OPTS[:num_worker] = v.to_i}
opt.on('-s [VAL]') {|v| OPTS[:num_server] = v.to_i}

opt.parse!(ARGV)
OPTS[:path_filename] = ARGV.pop
if OPTS[:path_filename] == nil
  puts "Usage: path_filename is required."
  exit
end

wd = WikiDump.new(OPTS[:path_filename], OPTS[:num_worker])

service = StatCollect.new
svr = MessagePack::RPC::Server.new
svr.listen("127.0.0.1", RPC_PORT, service)

job_start_date = Time.now

wd.nworker.times {|n|
  Process.fork {
    wd.dup
    wd.seek(n)
    count = 0
    rc = RiakUpload.new(OPTS[:num_server])
    printf "worker=%d: forked. start=%d, end=%d\n", n, wd.pos, wd.range_end(n)
    while article = wd.slice
      next if rc.obj_put("#{n}_#{count}", article) == nil
      count += 1
      printf "worker=%d: obj_put count=%d\n", n, count if count % 1000 == 0
      break if wd.pos > wd.range_end(n)
    end
    rpc_cli = MessagePack::RPC::Client.new("127.0.0.1", RPC_PORT)
    rpc_cli.timeout = 1
    rpc_cli.call(:report, count)
    rpc_cli.close
    printf "worker=%d: fork_end, total number of objects = %d\n", n, count
  }
}
Thread.fork {
  svr.run
}
Process.waitall

job_end_date = Time.now
elapsed_time = job_end_date - job_start_date

puts "Done."
printf "Total number of objects: %d\n", service.stats
printf "Elapsed time(sec): %f\n", elapsed_time
printf "CPS: %f\n", service.stats / elapsed_time

wd.close
