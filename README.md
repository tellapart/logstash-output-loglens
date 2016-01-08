# Loglens Logstash Output Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

## Documentation
### Environment
- To get started, you'll need JRuby with the Bundler gem installed.
- You'll also need maven since this plugin also contains Java code which is built using maven

- Install dependencies
```sh
bundle install
```
### Build and install Gem in Logstash
- Go to the root path of the Loglens plugin
- Compile the maven project

```sh
mvn compile
```
- Package the maven project
```sh
mvn package
```
- Build your plugin gem
```sh
gem build logstash-output-loglens.gemspec
```
- Install the plugin from the Logstash home
```sh
/path/to/logstash/bin/plugin install /your/local/plugin/logstash-output-loglens-0.0.1-java.gem
```
### Run the Loglens Logstash plugin
- Create Logstash config file, e.g.
```sh
input {
     file {
       path => "/vagrant/test.log"
     }
   }

   output {
     loglens {
       url => "the url"
       index => "loglens index"
       oauth2_token => "your oauth2 bearer token"
       is_debug => true #if true it will just print to stdout what it would send
     }
   }
```
- Start Logstash and proceed to test the plugin
```sh
/path/to/logstash/bin/bin/logstash -f <configfile>
```