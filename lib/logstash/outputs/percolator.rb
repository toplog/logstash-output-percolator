#encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "elasticsearch"
require "stud/buffer"

class LogStash::Outputs::Percolator < LogStash::Outputs::Base

  include Stud::Buffer

  config_name "percolator"
  milestone 1

  config :index, :validate => :string, :default => "logstash-%{+YYYY.MM.dd}"
  config :pattern_index, :validate => :string, :default => "logstash-patterns"
  config :host, :validate => :string
  config :port, :validate => :string, :default => "9200"
  config :redis_host, :validate => :array, :default => ["127.0.0.1"]
  config :redis_port, :validate => :number, :default => 6379
  config :redis_socket, :validate => :string
  config :db, :validate => :number, :default => 0
  config :password, :validate => :password
  config :key, :validate => :string, :required => false, :default => "percolator"
  config :data_type, :validate => [ "list", "channel" ], :required => false, :default => "list"
  config :reconnect_interval, :validate => :number, :default => 1
  config :timeout, :validate => :number, :default => 5
  config :congestion_threshold, :validate => :number, :default => 0
  config :congestion_interval, :validate => :number, :default => 1
  config :document_id, :validate => :string, :required => true

  public
  def register
    require 'redis'
    @logger.debug("Registered percolator plugin")
    @redis = nil
  end # def register

  public
  def receive(event)
    return unless output?(event)

    begin
      index = event.sprintf(@index)
      document_id = event.sprintf(@document_id)
      key = event.sprintf(@key)
      message = event["message"]
      @logger.debug("Percolator args are ", :host => @host, :redis_url => @redis_url, :port => @port, :redis_socket => @redis_socket, :key => @key, :index => index, :document_id => document_id, :percolate_index => @pattern_index)
      es_client = Elasticsearch::Client.new host: @host, port: @port
      get_index = es_client.indices.exists index: pattern_index
      unless get_index
        es_client.indices.create index: pattern_index
      end

      percolation = es_client.percolate index: pattern_index, type: 'automatic', body: { doc: { message: message } }
      alert_ids = []
      unless percolation['matches'].empty?
        percolation['matches'].each do | match |
          @logger.debug("#{document_id} matched #{match['_id']}")
          alert_ids << match['_id']
        end
        payload = {
          'matches' => matches,
          'document_id' => document_id
        }

        send_to_redis(payload.to_json)

      end
      event.remove("logstash_checksum")
      event.remove("random")
    rescue Exception => e
      @logger.debug(e.message)
    end
  end # def receive

  def send_to_redis(payload)
    begin
      @redis ||= connect
      if @data_type == 'list'
        congestion_check(key)
      @redis.rpush(key, payload)
      else
       @redis.publish(key, payload)
      end
    rescue => e
    @logger.warn("Failed to send event to Redis",
      :payload => payload,
      :identity => identity, :exception => e,
      :backtrace => e.backtrace)
    sleep @reconnect_interval
    @redis = nil
    retry
    end
  end

  def connect
    @host_idx = 0
    @current_host, @current_port = @redis_host[@host_idx].split(':')
    @host_idx = @host_idx + 1 >= @redis_host.length ? 0 : @host_idx + 1
    if not @current_port
      @current_port = @redis_port
    end
    if @redis_socket
      params = {
        :path => @redis_socket,
        :timeout => @timeout
      }
    else
      params = {
        :host => @current_host,
        :port => @current_port,
        :timeout => @timeout,
        :db => @db,
      }
    end
    @logger.debug(params)
    if @password
      params[:password] = @password.value
    end
    ::Redis.new(params)
  end # def connect

  def congestion_check(key)
  return if @congestion_threshold == 0
    if (Time.now.to_i - @congestion_check_times[key]) >= @congestion_interval # Check congestion only if enough time has passed since last check.
      while @redis.llen(key) > @congestion_threshold # Don't push event to Redis key which has reached @congestion_threshold.
        @logger.warn? and @logger.warn("Redis key size has hit a congestion threshold #{@congestion_threshold} suspending output for #{@congestion_interval} seconds")
        sleep @congestion_interval
      end
      @congestion_check_time = Time.now.to_i
    end
  end

  def identity
    @name || "redis://#{@password}@#{@current_host}:#{@current_port}/#{@db} #{@data_type}:#{@key}"
  end
end # class LogStash::Outputs::Percolator
