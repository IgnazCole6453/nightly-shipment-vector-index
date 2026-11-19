use crate::infrai::{ClientError, IndexedVector, InfraiClient};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

pub const COLLECTION: &str = "shipment-operations";
pub const EMBEDDING_DIMENSION: usize = 1536;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShipmentEventKind {
    PickedUp,
    InTransit,
    Delivered,
    Exception,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProofOfDelivery {
    pub document_url: String,
    pub signed_by: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShipmentEvent {
    pub shipment_id: String,
    pub occurred_at: String,
    pub kind: ShipmentEventKind,
    pub notes_url: String,
    pub proof: Option<ProofOfDelivery>,
    pub exception_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexDecision {
    OperationalHistory,
    ExceptionReview,
    SkipCompleted,
}

pub fn decide(event: &ShipmentEvent) -> IndexDecision {
    match (&event.kind, &event.proof, &event.exception_code) {
        (ShipmentEventKind::Delivered, Some(_), None) => IndexDecision::SkipCompleted,
        (ShipmentEventKind::Exception, _, _) | (ShipmentEventKind::Delivered, None, _) => {
            IndexDecision::ExceptionReview
        }
        _ => IndexDecision::OperationalHistory,
    }
}

#[derive(Debug, Deserialize)]
pub struct ReindexRequest {
    pub run_id: String,
    pub events: Vec<ShipmentEvent>,
}

#[derive(Debug, Serialize)]
pub struct ReindexResult {
    pub run_id: String,
    pub indexed: usize,
    pub skipped_completed: usize,
}

#[derive(Debug, Error)]
pub enum ReindexError {
    #[error("run_id and events must not be empty")]
    EmptyInput,
    #[error(transparent)]
    Infrai(#[from] ClientError),
}

pub async fn reindex(
    client: &InfraiClient,
    request: ReindexRequest,
) -> Result<ReindexResult, ReindexError> {
    if request.run_id.trim().is_empty() || request.events.is_empty() {
        return Err(ReindexError::EmptyInput);
    }
    let mut vectors = Vec::new();
    let mut skipped_completed = 0;
    for event in request.events {
        let decision = decide(&event);
        if decision == IndexDecision::SkipCompleted {
            skipped_completed += 1;
            continue;
        }
        let scraped = client.scrape(&event.notes_url).await?;
        let document = index_document(&event, &decision, &scraped);
        let embedding = client.embed(&document).await?;
        vectors.push(IndexedVector {
            id: format!("{}:{}", event.shipment_id, event.occurred_at),
            values: embedding,
            metadata: json!({
                "shipment_id": event.shipment_id,
                "occurred_at": event.occurred_at,
                "index_lane": match decision { IndexDecision::ExceptionReview => "exception_review", _ => "operational_history" },
                "proof": event.proof,
                "exception_code": event.exception_code,
            }),
        });
    }
    let indexed = vectors.len();
    if indexed > 0 {
        client.upsert(COLLECTION, vectors, &request.run_id).await?;
    }
    Ok(ReindexResult {
        run_id: request.run_id,
        indexed,
        skipped_completed,
    })
}

fn index_document(event: &ShipmentEvent, decision: &IndexDecision, scraped: &Value) -> String {
    format!(
        "shipment={} occurred_at={} kind={:?} lane={:?} source={}",
        event.shipment_id, event.occurred_at, event.kind, decision, scraped
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(
        kind: ShipmentEventKind,
        proof: Option<ProofOfDelivery>,
        exception_code: Option<&str>,
    ) -> ShipmentEvent {
        ShipmentEvent {
            shipment_id: "SHP-2048".into(),
            occurred_at: "2026-09-21T01:00:00Z".into(),
            kind,
            notes_url: "https://dispatch.example/events/SHP-2048".into(),
            proof,
            exception_code: exception_code.map(str::to_owned),
        }
    }

    #[test]
    fn keeps_exceptions_searchable_and_skips_completed_delivery() {
        let exception = event(ShipmentEventKind::Exception, None, Some("WEATHER_HOLD"));
        let delivered = event(
            ShipmentEventKind::Delivered,
            Some(ProofOfDelivery {
                document_url: "https://dispatch.example/pod/SHP-2048.pdf".into(),
                signed_by: "A. Chen".into(),
            }),
            None,
        );
        assert_eq!(decide(&exception), IndexDecision::ExceptionReview);
        assert_eq!(decide(&delivered), IndexDecision::SkipCompleted);
    }
}
