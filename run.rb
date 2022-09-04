#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)
require 'dotenv'
require 'pry'
require 'csv'
require 'pg'
Dotenv.load

clean_cmd = "psql discourse -f clean_all.sql"
puts clean_cmd 
system(clean_cmd) rescue nil

skip_list = %w[
  tx
  tx1
  one_row_table
  user_search_data
  user_options
  post_search_data
  post_action_types
  user_api_keys
  topic_search_data
]

csv_dir = ENV["CSV_DIR"] || "/shared/log/discourse_csv"
encoder = PG::TextEncoder::Array.new(name: "text[]", delimiter: ',')

Dir.glob("#{csv_dir}/*.000000000.csv").each do |file|
  table_name = File.basename(file).split(".")[1]
  if skip_list.include?(table_name)
    
    if table_name == 'user_api_keys'
      original_file = file 
      columns = ["id", "user_id", "client_id", "key", "application_name", "push_url", "created_at", "updated_at", "revoked_at", "scopes", "last_used_at"]
      file = file.gsub("000000000", "111111111")

      CSV.open(file, "w", headers: columns, write_headers: true, quote_char: "'", force_quotes: true, write_nil_value: '\N') do |csv_w|
        CSV.open(original_file,  headers: true, quote_char: "'", col_sep: ',', liberal_parsing: true).each do |csv_r|
          h = csv_r.to_h 
          h["scopes"] = encoder.encode(JSON.parse(h["scopes"]))
          csv_w << h.values_at(*columns)
        end
      end
    else
      puts "skipped #{table_name}"
      next
    end
  end
  begin 
    column_names = `head -1 #{file}`.strip.gsub("'", '\"')
    cmd = %Q{psql discourse -e -c "SET session_replication_role = replica;" -c "set role to discourse;" -c "\\copy #{table_name} (#{column_names}) FROM '#{file}' WITH HEADER NULL '\\N' DELIMITER E',' QUOTE E'\\'' ESCAPE E'\\\\\\\\' CSV;"}
    puts cmd 
    system(cmd)
  rescue 
    puts $!
  end
end