mod crawler;
mod proxy_list;
mod validator;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Fetching proxies...");
    let proxies = proxy_list::fetch_proxies().await?;
    println!("Fetched {} proxies", proxies.len());

    if let Some(proxy) = proxies.first() {
        let target_url = "https://www.basketball-reference.com/";
        println!("Validating proxy {} for {}", proxy, target_url);
        let ok = validator::validate(proxy, target_url).await?;
        println!("Proxy valid: {}", ok);

        if ok {
            println!("Crawling {} via proxy {}", target_url, proxy);
            let body = crawler::crawl_with_proxy(target_url, Some(proxy.clone())).await?;
            println!(
                "Response body (first 200 chars):\n{}",
                &body.chars().take(200).collect::<String>()
            );
        }
    } else {
        println!("No proxies found.");
    }

    Ok(())
}
