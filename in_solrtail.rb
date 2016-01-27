module Fluent
	require 'rubygems'
	require 'json'
    require 'fileutils'

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

		desc 'Chunk size to read from solr'
		config_param :chunk_size, :integer, :default => 100

        desc 'Solr tail log file full path'
        config_param :log_file, :string, :default => '/var/log/solrtail/solrtail.log'

		desc 'Solr required fields (comma separated)'
		config_param :required_fields, :string, :default => ''

		## Override configure to validate custom parameters
		def configure(conf)
			super
			@identifier_array = Array.new
			if @identifier.empty?
				raise ConfigError, 'Please specify the field identifier to pick value from a log entry'
			end
			if @identifier_key.empty?
				raise ConfigError, 'Please specify an identifier key to query solr'
			end
			if @solr_address.empty?
				raise ConfigError, 'Please specify the solr server address'
			end
			if @required_fields.empty?
				   raise ConfigError, 'Please specify required fields.'
            end

			##solrtail log file to log results.
			begin
                FileUtils.mkdir_p File.dirname(@log_file), :mode => 755
				file = File.open(@log_file, "w")
			rescue IOError => e
				raise ConfigError, e
			ensure
				file.close unless file.nil?
			end
		end

		def get_from_solr

			begin
				if @solr_proxy.empty?
					solr = RSolr.connect :url => @solr_address
				else
					solr = RSolr.connect :url => @solr_address, :proxy => @solr_proxy
				end
				query_value = @identifier_array.join(" OR ")
				log.info "Solr query value: #{query_value}"
				resp = solr.get 'select', :params => {:q => "#{identifier_key}:#{query_value}", :fl => "#{required_fields}"}
				resp['response']['docs'].each {
				if resp['response']['numFound'] > 0
                    objects = resp['response']['docs']
                    for object in objects
    					object = resp['response']['docs'][0].to_json
	    				aFile = File.new(@log_file, "a")
		    			if aFile
			    			aFile.syswrite("#{object}\n")
				    		aFile.close
					    else
						    puts "Unable to open file!"
					    end
                    end
				else
					log.warn "Document with (#{identifier_key}=#{identifier}) not found."
				end
				}
			rescue => e
				log.error "#{e}"
			ensure
				log.info "Cleaning chunk array"
				@identifier_array.clear
			end
		end

		## Oveerride convert lines method to implement
		## a hook to solr to get extra record information
		## from solr.
		def convert_line_to_event(line, es)
			begin
				line.chomp!  # remove \n
				@parser.parse(line) { |time, record|
					if time && record
						es.add(time, record)
						identifier = record[@identifier]
						if identifier
							@identifier_array.push(identifier)
							if @identifier_array.size >= chunk_size
								log.info "Identifier Array Count : #{@identifier_array.size}"
								get_from_solr
							end
						end
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
