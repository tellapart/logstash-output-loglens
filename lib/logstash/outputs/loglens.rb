# encoding: utf-8
require "logstash/namespace"
require "logstash/environment"
require "logstash/outputs/base"
require "logstash/json"
require 'json'
require 'securerandom'

require 'java'
require 'httpcore-4.2.4.jar'
require 'libthrift-0.9.1.jar'
require 'loglens-connector-0.0.1.jar'
java_import 'com.twitter.loglens.LoglensConnector'

# This is just a thin wrapper around the LoglensConnector java class
class LogStash::Outputs::LoglensPublic < LogStash::Outputs::Base
  config_name "loglens" #name of the logstash config section

  #Parameters you can specify in the logstash configuration
  config :url, :validate => :string
  config :index, :validate => :string
  config :oauth2_token, :validate => :string
  config :is_debug, :validate => :boolean

  @loglensConnector = nil

  #Initialize the LoglensConnector
  public
  def register
    $stdout.puts("Loglens Plugin Configuration")
    $stdout.puts("url: " + @url)
    $stdout.puts("loglens index: " + @index)
    $stdout.puts
    @loglensConnector = LoglensConnector.new(@url, @oauth2_token)
  end # def register

  public
  def receive(event)
    return if event == LogStash::SHUTDOWN

    loglensMessage = Hash.new
    loglensMessage["index"] = @index
    type = event["level"]
    loglensMessage["type"] = type ? type : "UNDEFINED"
    loglensMessage["id"] = SecureRandom.uuid
    loglensMessage["source"] = event

    if @is_debug
      @loglensConnector.printMessage(loglensMessage.to_json)
    else
      @loglensConnector.sendMessage(loglensMessage.to_json)
    end
  end # def event
end # class LogStash::Outputs::LoglensPublic
