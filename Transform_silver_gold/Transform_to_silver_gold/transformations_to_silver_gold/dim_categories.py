import dlt

# Create Empty Streaming Table
dlt.create_streaming_table(
  name="dim_categories"
)

# AUTO CDC FLOW
dlt.create_auto_cdc_flow(
  target = "dim_categories",
  source = "categories_silver_view",
  keys = ["CategoryID"],
  sequence_by = "inserted_datetime",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None
)
