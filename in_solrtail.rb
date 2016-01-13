module Fluent

require 'fluent/plugin/in_tail'
require 'fluent/mixin/config_placeholders'


class SolrTailInput < TailInput
	Plugin.register_input('solrtail', self)
	def convert_line_to_event(line, es)
		 begin
			line.chomp!  # remove \n
			@parser.parse(line) { |time, record|
				if time && record

				############changes to hit solr#############
				############################################

					es.add(time, record)
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

