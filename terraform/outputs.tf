output "raw_dataset_id" {
  value = google_bigquery_dataset.raw_gharchive.dataset_id
}

output "raw_events_table_id" {
  value = "${google_bigquery_dataset.raw_gharchive.dataset_id}.${google_bigquery_table.events.table_id}"
}
