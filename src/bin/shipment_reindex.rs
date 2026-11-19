use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use nightly_shipment_index::{
    infrai::{ClientError, InfraiClient},
    shipment_index::{
        reindex, ReindexError, ReindexRequest, ShipmentEvent, ShipmentEventKind, COLLECTION,
        EMBEDDING_DIMENSION,
    },
};
use serde_json::json;
use std::{env, net::SocketAddr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = InfraiClient::from_env()?;
    match env::args().nth(1).as_deref() {
        Some("setup") => {
            let task = env::var("REINDEX_TASK_URL")?;
            client
                .create_collection(COLLECTION, EMBEDDING_DIMENSION)
                .await?;
            let job = client.create_schedule("0 2 * * *", &task).await?;
            println!("nightly reindex scheduled: {}", job.job_id);
        }
        _ => {
            let port = env::var("PORT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(3000);
            let app = Router::new()
                .route("/reindex/nightly", post(nightly))
                .with_state(client);
            let address = SocketAddr::from(([0, 0, 0, 0], port));
            println!("shipment reindex listening on {address}");
            axum::serve(tokio::net::TcpListener::bind(address).await?, app).await?;
        }
    }
    Ok(())
}

async fn nightly(
    State(client): State<InfraiClient>,
    request: Option<Json<ReindexRequest>>,
) -> Result<Json<nightly_shipment_index::shipment_index::ReindexResult>, ServiceError> {
    let request = match request {
        Some(Json(request)) => request,
        None => scheduled_request()?,
    };
    Ok(Json(reindex(&client, request).await?))
}

fn scheduled_request() -> Result<ReindexRequest, ServiceError> {
    let notes_url = env::var("SHIPMENT_FEED_URL").map_err(|_| ServiceError::MissingFeed)?;
    Ok(ReindexRequest {
        run_id: "nightly-shipment-feed".into(),
        events: vec![ShipmentEvent {
            shipment_id: "shipment-feed".into(),
            occurred_at: "scheduled".into(),
            kind: ShipmentEventKind::Exception,
            notes_url,
            proof: None,
            exception_code: Some("OPERATIONS_REVIEW".into()),
        }],
    })
}

enum ServiceError {
    Reindex(ReindexError),
    MissingFeed,
}

impl From<ReindexError> for ServiceError {
    fn from(value: ReindexError) -> Self {
        Self::Reindex(value)
    }
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        let status = match &self {
            ServiceError::MissingFeed | ServiceError::Reindex(ReindexError::EmptyInput) => {
                StatusCode::BAD_REQUEST
            }
            ServiceError::Reindex(ReindexError::Infrai(ClientError::Rejected {
                status, ..
            })) => StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST),
            _ => StatusCode::BAD_GATEWAY,
        };
        let message = match self {
            ServiceError::Reindex(error) => error.to_string(),
            ServiceError::MissingFeed => "SHIPMENT_FEED_URL is missing".into(),
        };
        (status, Json(json!({"ok": false, "error": message}))).into_response()
    }
}
