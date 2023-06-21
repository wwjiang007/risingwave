package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "pbdatagen/alltypes/proto" // replace with your actual path
)

func main() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Initialize a few records
	records := make([]*pb.AllTypes, 5)
	for i := 0; i < 5; i++ {
		records[i] = randomRecord()
	}

	// Write records to file
	writeRecordsToFile(records, "records.bin")
}

func writeRecordsToFile(records []*pb.AllTypes, filename string) {
	var out []byte

	// Create file
	for _, record := range records {
		data, err := proto.Marshal(record)
		if err != nil {
			log.Fatalln("Failed to encode records:", err)
		}

		out = append(out, data...)
	}

	// Write to file
	if err := ioutil.WriteFile(filename, out, 0644); err != nil {
		log.Fatalln("Failed to write records to file:", err)
	}

	fmt.Println("Records have been written to", filename)
}

func randomRecord() *pb.AllTypes {
	// Create timestamp
	timestamp := timestamppb.New(time.Now())

	// Create duration
	duration := durationpb.New(time.Duration(rand.Int63()))

	// Create any
	a := &any.Any{
		TypeUrl: "example.com/example",
		Value:   []byte("Example Value"),
	}

	// Create a record
	return &pb.AllTypes{
		DoubleField:   rand.Float64(),
		FloatField:    float32(rand.Float32()),
		Int32Field:    rand.Int31(),
		Int64Field:    rand.Int63(),
		Uint32Field:   uint32(rand.Uint32()),
		Uint64Field:   rand.Uint64(),
		Sint32Field:   rand.Int31(),
		Sint64Field:   rand.Int63(),
		Fixed32Field:  uint32(rand.Uint32()),
		Fixed64Field:  rand.Uint64(),
		Sfixed32Field: rand.Int31(),
		Sfixed64Field: rand.Int63(),
		BoolField:     rand.Int31()%2 == 0,
		StringField:   fmt.Sprintf("Random string %d", rand.Int31()),
		BytesField:    []byte(fmt.Sprintf("Random bytes %d", rand.Int31())),
		EnumField:     pb.AllTypes_EnumType(rand.Intn(3)),
		NestedMessageField: &pb.AllTypes_NestedMessage{
			Id:   rand.Int31(),
			Name: fmt.Sprintf("Nested name %d", rand.Int31()),
		},
		RepeatedIntField: []int32{rand.Int31(), rand.Int31(), rand.Int31()},
		ExampleOneof: &pb.AllTypes_OneofString{
			OneofString: fmt.Sprintf("Oneof string %d", rand.Int31()),
		},
		MapField: map[string]int32{
			"key1": rand.Int31(),
			"key2": rand.Int31(),
		},
		TimestampField: timestamp,
		DurationField:  duration,
		AnyField:       a,
	}
}
