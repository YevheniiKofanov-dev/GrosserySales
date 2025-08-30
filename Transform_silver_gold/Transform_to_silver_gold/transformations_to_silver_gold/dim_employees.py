import dlt

dlt.create_streaming_table(
    name="dim_employees"
)

dlt.create_auto_cdc_flow(
    target="dim_employees",
    source="employees_silver_view",
    keys=["EmployeeID"],
    sequence_by="inserted_datetime",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2,
    track_history_column_list=None,
    track_history_except_column_list=None
)
