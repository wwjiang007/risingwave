CREATE SOURCE twitter WITH (
    connector = 'kafka',
    topic = 'alltypes',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT PROTOBUF MESSAGE 'alltypes.schema.AllTypes' ROW SCHEMA LOCATION 'http://file_server:8080/alltypes_schema';
