#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)
require 'dotenv'
require 'pry'
require 'csv'
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

Dir.glob("#{csv_dir}/*.csv").each do |file|
  table_name = File.basename(file).split(".")[1]
  if skip_list.include?(table_name)
    puts "skipped #{table_name}"
    next
  end
  begin 
    column_names = `head -1 #{file}`.strip
    cmd = %Q{psql discourse -e -c "SET session_replication_role = replica;" -c "set role to discourse;" -c "\\copy #{table_name} (#{column_names}) FROM '#{file}' WITH HEADER NULL '\\N' DELIMITER E',' QUOTE E'\\'' ESCAPE E'\\\\\\\\' CSV;"}
    puts cmd 
    system(cmd)
  rescue 
    puts $!
  end
end