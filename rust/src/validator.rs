use reqwest::{Client, Proxy};
use std::error::Error;
use std::time::Duration;

/// Tests a single proxy by attempting to GET the target URL through it.
/// Returns true if the request succeeds within timeout.
pub async fn validate(proxy: &str, target_url: &str) -> Result<bool, Box<dyn Error>> {
    let proxy_uri = format!("http://{}", proxy);
    let client = Client::builder()
        .proxy(Proxy::all(proxy_uri)?)
        .timeout(Duration::from_secs(5))
        .build()?;
    match client.get(target_url).send().await {
        Ok(resp) => Ok(resp.status().is_success()),
        Err(_) => Ok(false),
    }
}
