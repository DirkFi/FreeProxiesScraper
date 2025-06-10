use reqwest::{Client, Proxy};
use std::error::Error;
use std::time::Duration;

/// Crawls the given URL, optionally via the provided proxy.
pub async fn crawl_with_proxy(url: &str, proxy: Option<String>) -> Result<String, Box<dyn Error>> {
    let mut builder = Client::builder().timeout(Duration::from_secs(10));
    if let Some(p) = proxy {
        builder = builder.proxy(Proxy::all(format!("http://{}", p))?);
    }
    let client = builder.build()?;
    let resp = client.get(url).send().await?;
    Ok(resp.text().await?)
}
