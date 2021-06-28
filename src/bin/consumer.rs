use log::{debug, error, info};
use rusoto_core::HttpClient;
use rusoto_credential::ProfileProvider;
use rusoto_sqs::{DeleteMessageRequest, Message, ReceiveMessageRequest, Sqs, SqsClient};
use settings::Settings;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, Sender as MpscSender};
use tokio::sync::watch::{self, Receiver, Sender as WatchSender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

type MessageQueue = Arc<Mutex<VecDeque<Message>>>;

#[tokio::main]
async fn main() {
    env_logger::init();
    let settings = Settings::new().expect("Valid settings");
    let client = Client {
        aws_credentials_profile: settings.aws.credentials_profile,
        region: settings.aws.region,
        queue_url: settings.aws.queue_url,
    };

    let message_queue = Arc::new(Mutex::new(VecDeque::new()));
    let (fetcher_tx, fetcher_rx) = watch::channel(true);

    // Maximise worker utilisation via concurrent fetching
    let fetcher_count = (settings.consumer.worker_count / 10) + 1;
    info!("Spawning {} fetchers", fetcher_count);
    for _ in 0..fetcher_count {
        spawn_fetcher(client.clone(), message_queue.clone(), fetcher_rx.clone());
    }

    // Spawn worker manager
    spawn_manager(
        client.clone(),
        settings.consumer.worker_count,
        message_queue.clone(),
        fetcher_tx,
    ).await.unwrap();
}

fn spawn_manager(
    client: Client,
    worker_count: usize,
    message_queue: MessageQueue,
    fetcher_tx: WatchSender<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (worker_tx, mut worker_rx) = mpsc::channel(worker_count);
        let mut idle_worker_ids = HashSet::new();

        for id in 0..worker_count {
            idle_worker_ids.insert(id);
            spawn_waiter(id, worker_tx.clone());
        }

        loop {
            match worker_rx.recv().await {
                Some(worker_id) => {
                    // Scope to minimise time spent with mutex lock
                    let message;
                    let queue_len;
                    {
                        let mut queue = message_queue.lock().expect("Mutex lock");
                        message = queue.pop_front();
                        queue_len = Some(queue.len());
                    }

                    // Process message if any
                    match message {
                        Some(message) => {
                            spawn_worker(worker_id, message, client.clone(), worker_tx.clone());
                            idle_worker_ids.remove(&worker_id);
                        }
                        None => {
                            spawn_waiter(worker_id, worker_tx.clone());
                            idle_worker_ids.insert(worker_id);
                        }
                    }

                    // Fetch more messages if required
                    if let Some(queue_len) = queue_len {
                        let should_fetch = queue_len < idle_worker_ids.len();
                        if let Err(e) = fetcher_tx.send(should_fetch) {
                            error!("[manager] Error sending queue size to fetcher: {:?}", e);
                        }
                        let worker_utilization =
                            ((worker_count - idle_worker_ids.len()) * 100) / worker_count;
                        info!(
                            "[manager] worker utilization {:>3}%, queue length: {}",
                            worker_utilization, queue_len
                        );
                    }
                }
                None => {
                    error!("Channel closed");
                }
            }
        }
    })
}

fn spawn_fetcher(
    client: Client,
    message_queue: MessageQueue,
    mut rx: Receiver<bool>,
) {
    tokio::spawn(async move {
        loop {
            // Only sees the latest value
            match rx.changed().await {
                Ok(_) => {
                    if *rx.borrow() == true {
                        info!("[fetcher] Long polling for new messages ...");
                        if let Some(messages) = client.fetch_messages().await {
                            info!("[fetcher] Fetched {} messages", messages.len());
                            let mut queue = message_queue.lock().expect("mutex lock");
                            for msg in messages {
                                queue.push_back(msg);
                            }
                        }
                    }
                }
                Err(e) => error!("[fetcher] channel receive error: {:?}", e),
            }
        }
    });
}

fn spawn_waiter(id: usize, tx: MpscSender<usize>) {
    tokio::spawn(async move {
        let _ = tokio::spawn(async move {
            debug!("[worker:{}] Idling ...", id);
            sleep(Duration::from_secs(5)).await;
        })
        .await;
        if tx.send(id).await.is_err() {
            error!("[worker:{}] Connection task shutdown", id);
        }
    });
}

fn spawn_worker(id: usize, message: Message, client: Client, tx: MpscSender<usize>) {
    tokio::spawn(async move {
        let res = tokio::spawn(async move {
            // Do some work
            sleep(Duration::from_secs(1)).await;
            if let Some(receipt_handle) = message.receipt_handle {
                client.delete_message(&receipt_handle).await;
            }
        })
        .await;
        match res {
            Ok(_) => debug!("[worker:{}] Completed work", id),
            Err(e) => error!("[worker:{}] Error: {:?}", id, e),
        }
        if tx.send(id).await.is_err() {
            error!("[worker:{}] Connection task shutdown", id);
        }
    });
}

#[derive(Clone)]
struct Client {
    aws_credentials_profile: String,
    region: String,
    queue_url: String,
}

impl Client {
    fn create(&self) -> SqsClient {
        let provider =
            ProfileProvider::with_default_credentials(self.aws_credentials_profile.clone())
                .expect("AWS credentials profile");
        SqsClient::new_with(
            HttpClient::new().expect("failed to create request dispatcher"),
            provider,
            self.region.parse().expect("Valid AWS region"),
        )
    }

    async fn fetch_messages(&self) -> Option<Vec<Message>> {
        let request = ReceiveMessageRequest {
            queue_url: self.queue_url.clone(),
            max_number_of_messages: Some(10),
            wait_time_seconds: Some(20),
            message_attribute_names: Some(vec!["All".to_string()]),
            ..Default::default()
        };

        let client = self.create();
        let response = client.receive_message(request).await;
        let result = match response {
            Ok(result) => result,
            Err(e) => {
                error!("Expected message result but got error: {:?}", e);
                return None;
            }
        };

        result.messages
    }

    async fn delete_message(&self, receipt_handle: &str) {
        let delete_message_request = DeleteMessageRequest {
            queue_url: self.queue_url.clone(),
            receipt_handle: receipt_handle.to_string(),
        };
        if let Err(e) = self.create().delete_message(delete_message_request).await {
            error!(
                "Couldn't delete message with receipt handle: {} error:{}",
                receipt_handle, e
            );
        }
    }
}
