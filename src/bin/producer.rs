use log::{debug, info};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ProfileProvider;
use rusoto_sqs::{SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs, SqsClient};
use settings::Settings;

#[tokio::main]
async fn main() {
    env_logger::init();
    let settings = Settings::new().expect("Valid settings");

    let mut requests = create_send_message_batch_requests(
        settings.producer.message_volume,
        10,
        &settings.aws.queue_url,
    );

    let region: Region = settings.aws.region.parse().expect("Valid AWS region");

    while !requests.is_empty() {
        info!("Commencing concurrent send message requests ...");
        // 10 concurrent requests each with a max of 10 messages
        tokio::join!(
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
            send_request(requests.pop(), &settings.aws.credentials_profile, &region),
        );
    }
}
async fn send_request(
    request: Option<SendMessageBatchRequest>,
    aws_cred_profile: &str,
    aws_region: &Region,
) {
    if let Some(request) = request {
        let request_len = request.entries.len();
        let provider = ProfileProvider::with_default_credentials(aws_cred_profile.to_string())
            .expect("AWS credentials profile");
        let client = SqsClient::new_with(
            HttpClient::new().expect("failed to create request dispatcher"),
            provider,
            aws_region.clone(),
        );
        // let client = create_sqs_client(aws_cred_profile.to_string(), aws_region.clone());
        let response = client.send_message_batch(request).await;
        info!("Produced {} messages", request_len);
        debug!("Produced messages {:#?}", response.unwrap());
    }
}

fn create_send_message_batch_requests(
    total_messages: usize,
    max_messages_per_batch: usize,
    queue_url: &str,
) -> Vec<SendMessageBatchRequest> {
    assert!(max_messages_per_batch > 0);

    let mut entries_per_request: Vec<usize> =
        vec![10; total_messages as usize / max_messages_per_batch as usize];
    if total_messages % max_messages_per_batch != 0 {
        entries_per_request.push(total_messages % max_messages_per_batch);
    }

    entries_per_request
        .into_iter()
        .map(|count| SendMessageBatchRequest {
            queue_url: String::from(queue_url),
            entries: create_send_message_batch_request_entries(count),
            ..Default::default()
        })
        .collect()
}

fn create_send_message_batch_request_entries(size: usize) -> Vec<SendMessageBatchRequestEntry> {
    let ids: Vec<usize> = (0..size).collect();
    let msg_str = String::from("lorem ipsum dolor sit amet");
    ids.into_iter()
        .map(|id| SendMessageBatchRequestEntry {
            id: id.to_string(),
            message_body: msg_str.clone(),
            ..Default::default()
        })
        .collect()
}

pub fn create_sqs_client(cred_profile: String, region: Region) -> SqsClient {
    let provider =
        ProfileProvider::with_default_credentials(cred_profile).expect("AWS credentials profile");
    SqsClient::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        provider,
        region,
    )
}
