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
  user_api_keys
  topic_search_data
]

csv_dir = ENV["CSV_DIR"] || "/shared/discourse_csv"
encoder = PG::TextEncoder::Array.new(name: "text[]", delimiter: ',')

Dir.glob("#{csv_dir}/*.000000000.csv").each do |file|
  table_name = File.basename(file).split(".")[1]
  if skip_list.include?(table_name)
    
    if table_name == 'user_api_keys'
      original_file = file 
      columns = ["id", "user_id", "client_id", "key", "application_name", "push_url", "created_at", "updated_at", "revoked_at", "scopes", "last_used_at"]
      file = file.gsub("000000000", "111111111")

      CSV.open(file, "w", headers: columns, write_headers: true, quote_char: "'", write_nil_value: '\N') do |csv_w|
        CSV.open(original_file,  headers: true, quote_char: "'", col_sep: ',', liberal_parsing: true).each do |csv_r|
          h = csv_r.to_h 
          h["scopes"] = encoder.encode(JSON.parse(h["scopes"]))
          csv_w << h.values_at(*columns)
        end
      end
    elsif table_name == 'user_options'
      original_file = file 
      columns = ["user_id",
        "mailing_list_mode",
        "email_digests",
        "external_links_in_new_tab",
        "enable_quoting",
        "dynamic_favicon",
        "disable_jump_reply",
        "automatically_unpin_topics",
        "digest_after_minutes",
        "auto_track_topics_after_msecs",
        "new_topic_duration_minutes",
        "last_redirected_to_top_at",
        "email_previous_replies",
        "email_in_reply_to",
        "like_notification_frequency",
        "mailing_list_mode_frequency",
        "include_tl0_in_digests",
        "notification_level_when_replying",
        "theme_key_seq",
        "allow_private_messages",
        "homepage_id",
        "theme_ids",
        "hide_profile_and_presence",
        "text_size_key",
        "text_size_seq",
        "email_level",
        "email_messages_level",
        "title_count_mode_key",
        "enable_defer"
      ]

      file = file.gsub("000000000", "111111111")

      CSV.open(file, "w", headers: columns, write_headers: true, quote_char: "'", write_nil_value: '\N') do |csv_w|
        CSV.open(original_file,  headers: true, quote_char: "'", col_sep: ',', liberal_parsing: true).each do |csv_r|
          h = csv_r.to_h 
          h["theme_ids"] = encoder.encode(JSON.parse(h["theme_ids"]))
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
    cmd = %Q{psql discourse -e -c "SET session_replication_role = replica;" -c "set role to discourse;" -c "SET backslash_quote = 'on';" -c "\\copy #{table_name} (#{column_names}) FROM '#{file}' WITH HEADER NULL '\\N' DELIMITER E',' QUOTE E'\\'' ESCAPE E'\\\\\\\\' CSV;"}
    puts cmd 
    system(cmd)
  rescue 
    puts $!
  end
end

puts "sync seq..."

["schema_migration_details_id_seq",
  "topics_id_seq",
  "posts_id_seq",
  "users_id_seq",
  "uploads_id_seq",
  "topic_links_id_seq",
  "message_bus_id_seq",
  "notifications_id_seq",
  "incoming_links_id_seq",
  "categories_id_seq",
  "site_settings_id_seq",
  "user_open_ids_id_seq",
  "user_actions_id_seq",
  "post_actions_id_seq",
  "topic_link_clicks_id_seq",
  "post_action_types_id_seq",
  "email_logs_id_seq",
  "topic_allowed_users_id_seq",
  "invites_id_seq",
  "topic_invites_id_seq",
  "user_visits_id_seq",
  "email_tokens_id_seq",
  "drafts_id_seq",
  "themes_id_seq",
  "draft_sequences_id_seq",
  "github_user_infos_id_seq",
  "user_histories_id_seq",
  "groups_id_seq",
  "group_users_id_seq",
  "category_groups_id_seq",
  "topic_allowed_groups_id_seq",
  "post_uploads_id_seq",
  "optimized_images_id_seq",
  "category_featured_topics_id_seq",
  "topic_users_id_seq",
  "screened_emails_id_seq",
  "screened_urls_id_seq",
  "oauth2_user_infos_id_seq",
  "plugin_store_rows_id_seq",
  "post_details_id_seq",
  "screened_ip_addresses_id_seq",
  "api_keys_id_seq",
  "post_revisions_id_seq",
  "topic_embeds_id_seq",
  "top_topics_id_seq",
  "category_users_id_seq",
  "single_sign_on_records_id_seq",
  "badge_types_id_seq",
  "badges_id_seq",
  "user_badges_id_seq",
  "color_schemes_id_seq",
  "color_scheme_colors_id_seq",
  "user_custom_fields_id_seq",
  "category_custom_fields_id_seq",
  "group_custom_fields_id_seq",
  "post_custom_fields_id_seq",
  "topic_custom_fields_id_seq",
  "invited_groups_id_seq",
  "google_user_infos_id_seq",
  "user_avatars_id_seq",
  "quoted_posts_id_seq",
  "badge_groupings_id_seq",
  "incoming_referers_id_seq",
  "incoming_domains_id_seq",
  "topic_search_data_topic_id_seq",
  "permalinks_id_seq",
  "user_warnings_id_seq",
  "user_fields_id_seq",
  "user_exports_id_seq",
  "application_requests_id_seq",
  "directory_items_id_seq",
  "muted_users_id_seq",
  "stylesheet_cache_id_seq",
  "user_field_options_id_seq",
  "post_stats_id_seq",
  "embeddable_hosts_id_seq",
  "user_profile_views_id_seq",
  "translation_overrides_id_seq",
  "group_mentions_id_seq",
  "user_archived_messages_id_seq",
  "group_archived_messages_id_seq",
  "incoming_emails_id_seq",
  "instagram_user_infos_id_seq",
  "email_change_requests_id_seq",
  "onceoff_logs_id_seq",
  "tags_id_seq",
  "topic_tags_id_seq",
  "tag_users_id_seq",
  "category_tags_id_seq",
  "scheduler_stats_id_seq",
  "tag_groups_id_seq",
  "tag_group_memberships_id_seq",
  "category_tag_groups_id_seq",
  "developers_id_seq",
  "user_api_keys_id_seq",
  "web_hook_event_types_id_seq",
  "web_hooks_id_seq",
  "web_hook_events_id_seq",
  "group_histories_id_seq",
  "user_auth_tokens_id_seq",
  "custom_emojis_id_seq",
  "user_auth_token_logs_id_seq",
  "child_themes_id_seq",
  "topic_timers_id_seq",
  "theme_fields_id_seq",
  "remote_themes_id_seq",
  "watched_words_id_seq",
  "search_logs_id_seq",
  "user_emails_id_seq",
  "tag_search_data_tag_id_seq",
  "user_second_factors_id_seq",
  "theme_settings_id_seq",
  "category_tag_stats_id_seq",
  "web_crawler_requests_id_seq",
  "tag_group_permissions_id_seq",
  "shared_drafts_id_seq",
  "push_subscriptions_id_seq",
  "post_reply_keys_id_seq",
  "skipped_email_logs_id_seq",
  "polls_id_seq",
  "poll_options_id_seq",
  "user_uploads_id_seq",
  "javascript_caches_id_seq",
  "user_associated_accounts_id_seq",
  "theme_translation_overrides_id_seq",
  "reviewables_id_seq",
  "reviewable_histories_id_seq",
  "group_requests_id_seq",
  "reviewable_scores_id_seq",
  "ignored_users_id_seq",
  "reviewable_claimed_topics_id_seq",
  "anonymous_users_id_seq"
].each do |seq|
  table_name = seq.sub("_id_seq", "")
  cmd = %Q{psql discourse  -c "set role to discourse;" -c "SELECT SETVAL('#{seq}', COALESCE(MAX(id)+1, 1) ) FROM #{table_name};"}
  puts cmd 
  system(cmd)
end
