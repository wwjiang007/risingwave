use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Field;
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct FlightServiceImpl {}

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + 'static>>;

#[async_trait::async_trait]
impl FlightService for FlightServiceImpl {
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListActionsStream = TonicStream<ActionType>;
    type ListFlightsStream = TonicStream<FlightInfo>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let request = request.into_inner().message().await?.unwrap();
        let response = HandshakeResponse {
            protocol_version: request.protocol_version,
            payload: request.payload,
        };
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights Not yet implemented"))
    }

    // create function foo(int) returns int as 'foo' using link 'http://localhost:6666';

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = arrow_schema::Schema::new(vec![
            Field::new("a", arrow_schema::DataType::Utf8, true),
            Field::new("b", arrow_schema::DataType::Utf8, true),
            Field::new("out", arrow_schema::DataType::Int64, true),
        ]);
        let schema: SchemaResult = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .unwrap();

        let info = FlightInfo {
            schema: schema.schema,
            total_records: 2,
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema Not yet implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get Not yet implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let schema =
            arrow_schema::Schema::new(vec![Field::new("a", arrow_schema::DataType::Utf8, false)]);

        let mut stream = request.into_inner();
        // Decode stream of FlightData to RecordBatches
        let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            // convert tonic::Status to FlightError
            stream.map_err(|e| e.into()),
        );

        let mut ret =
            arrow_array::builder::PrimitiveBuilder::<arrow_array::types::Int64Type>::new();

        // Read back RecordBatches
        while let Some(batch) = record_batch_stream.next().await {
            match batch {
                Ok(batch) => {
                    // process batch
                    // println!("Received batch: {:?}", batch);
                    count_char_chunk(batch, &mut ret);
                }
                Err(e) => { /* handle error */ }
            };
        }

        let ret = ret.finish();
        let out_schema =
            arrow_schema::Schema::new(vec![Field::new("a", arrow_schema::DataType::Int64, false)]);

        let chunk = RecordBatch::try_new(Arc::new(out_schema), vec![Arc::new(ret)]).unwrap();

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(futures::stream::iter(vec![Ok(chunk)]))
            .map_err(|e| Status::internal(e.to_string()));

        // Create a tonic `Response` that can be returned from a Flight server
        let response = Response::new(flight_data_stream.boxed());
        Ok(response)
    }
}

fn count_char(s: &str, char: &str) -> i64 {
    let mut count = 0;
    let char = char.bytes().next().unwrap();

    for c in s.bytes() {
        if c == char {
            count += 1;
        }
    }
    count
}

fn count_char_chunk(
    batch: RecordBatch,
    ret: &mut arrow_array::builder::PrimitiveBuilder<arrow_array::types::Int64Type>,
) {
    for i in 0..batch.num_rows() {
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect(
                format!(
                    "expected StringArray, got {:?}",
                    batch.column(0).data_type()
                )
                .as_str(),
            )
            .value(i);
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect(
                format!(
                    "expected StringArray, got {:?}",
                    batch.column(1).data_type()
                )
                .as_str(),
            )
            .value(i);
        ret.append_value(count_char(s, c));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:8815".parse()?;
    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);
    println!("Flight service listening on {}", addr);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
