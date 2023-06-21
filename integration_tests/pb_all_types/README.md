## Updating the proto file

'alltypes_schema' is the file descriptor for 'alltypes.proto'. When you update the proto file, you will need to update the file descriptor accordingly via:

```sh
protoc --include_imports --descriptor_set_out=alltypes_schema ./alltypes.proto
```

Similarly, you will also need to reproduce the golang file `alltypes.pb.go`. You can run protoc to regenerate the file:

```sh
protoc --proto_path=./ alltypes.proto --go_out=./pbdatagen
```
