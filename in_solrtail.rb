module Fluent
	require 'rubygems'
	require 'json'

	## Local imports
	require '/var/lib/gems/1.9.1/gems/rsolr-1.0.13/lib/rsolr.rb'
	require 'fluent/plugin/in_tail'
	require 'fluent/mixin/config_placeholders'


	class SolrTailInput < NewTailInput

		## Register the plugin
		Plugin.register_input('solrtail', self)

		## Define custom parameters
		desc 'Solr server address'
		config_param :solr_address, :string, :default => ''

		desc 'Solr proxy address'
		config_param :solr_proxy, :string, :default => ''

		desc 'Field name from the log entries whose value has to be used to query solr'
		config_param :identifier, :string, :default => ''

		desc 'Identifier key that will be used against identifier value to query solr'
		config_param :identifier_key, :string, :default => ''

		## Override configure to validate custom parameters
		def configure(conf)
			super
			if @identifier.empty?
				raise ConfigError, 'Please specify the field identifier to pick value from a log entry'
			end
			if @identifier_key.empty?
				raise ConfigError, 'Please specify an identifier key to query solr'
			end
			if @solr_address.empty?
				raise ConfigError, 'Please specify the solr server address'
			end

			##TODO: create a solrtail log file to log results.
		end

		## Oveerride convert lines method to implement
		## a hook to solr to get extra record information
		## from solr.
		def convert_line_to_event(line, es)
			begin
				line.chomp!  # remove \n
				@parser.parse(line) { |time, record|
					if time && record
						identifier = record[@identifier]
						## only if the identifier is available, we are going
						## to query the solr server else we are just gonna
						## log the record and continue with parsing.
						if identifier

							## Try connecting to the solr server
							if @solr_proxy.empty?
								solr = RSolr.connect :url => @solr_address
							else
								solr = RSolr.connect :url => @solr_address, :proxy => @solr_proxy
							end

							resp = solr.get 'select', :params => {:q => "#{identifier_key}:#{identifier}"}
							if resp['response']['numFound'] > 0
								object = resp['response']['docs'][0].to_json
								log.info object

								## TODO: write json to logfile
							else
								log.warn "Document with (#{identifier_key}=#{identifier}) not found."
							end
						else
							log.info "identifier not found: #{line.inspect}"	
						end
						#es.add(time, record)
					else
						log.warn "pattern not match: #{line.inspect}"
					end
				}
			rescue => e
				log.warn line.dump, :error => e.to_s
				log.debug_backtrace(e.backtrace)
			end
		end

	end
end
