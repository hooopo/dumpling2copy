#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)
require 'dotenv'
require 'pry'
require 'csv'
Dotenv.load

skip_list = %w[
  tx
  tx1
]

csv_dir = ENV["CSV_DIR"] || "./discourse_csv"

Dir.glob("#{csv_dir}/*.csv").each do |file|
  table_name = File.basename(file).split(".")[1]
  if skip_list.include?(table_name)
    puts "skipped #{table_name}"
    next
  end
  begin 
    cmd = %Q{psql -c "\\copy #{table_name} FROM '#{file}' WITH HEADER NULL '\N' DELIMITER E',' QUOTE E'\'' ESCAPE E'\\' CSV;"}
    puts cmd 
    system(cmd)
  rescue 
    puts $!
  end
end