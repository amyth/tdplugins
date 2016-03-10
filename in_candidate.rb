module Fluent

	## Gem imports
	require 'rubygems'
	require 'json'
	require 'fileutils'

	## Local imports
	require 'fluent/plugin/in_tail'
	require 'fluent/mixin/config_placeholders'


	class CandidateInput < NewTailInput

		## Register the plugin
		Plugin.register_input('candidate', self)

		## Define custom parameters

		desc 'Chunk size to read from solr'
		config_param :chunk_size, :integer, :default => 100

		desc 'Solr tail log file full path'
		config_param :log_file, :string, :default => '/var/log/solrtail/solrtail.log'

		desc 'List of fields to replace with nil if these are blank strings'
		config_param :replace_with_nil, :string, :default => ''


		## Override configure to validate custom parameters
		def configure(conf)
			super
			@identifier_array = Array.new
			if @identifier.empty?
				raise ConfigError, 'Please specify the field identifier to pick value from a log entry'
			end
			##solrtail log file to log results.
			begin
                FileUtils.mkdir_p File.dirname(@log_file), :mode => 755
				file = File.open(@log_file, "a")
			rescue IOError => e
				raise ConfigError, e
			ensure
				file.close unless file.nil?
			end

		end


		def get_from_api

			begin
				query_value = @identifier_array.join(",")

				#################################TO BE CHANGED TO QUERY THE API################################

				#resp = @solr.get 'select', :params => {:q => "#{identifier_key}:( #{query_value} )", :fl => "#{required_fields}", :rows => "#{chunk_size}"}
				return resp
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
						identifier = record[@identifier].gsub(/\s+/, '')
						if identifier && identifier.length > 4
							@identifier_array.push("#{identifier}")
							if @identifier_array.size >= chunk_size
								resp = get_from_api
								log_from_response(resp)
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

		## Logs the data to the out put file using the given response.
		def log_from_response response
			aFile = File.new(@log_file, "a")

				########CHANGE ACCORDING TO THE FORMAT OF RECORDS FETCHED FROM API###########################################

				for object in response['response']['docs']
					if !@replace_with_nil.empty?
						for rfield in @replace_with_nil.split(",")
							if object.has_key?(rfield) && object[rfield] == ""
								object[rfield] = nil
							end
						end
					end
					aFile.syswrite("#{object.to_json}\n")
				end
            aFile.close
		end
	end
end
