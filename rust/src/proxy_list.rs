use reqwest::Client;
use scraper::{Html, Selector};
use std::error::Error;

/// Fetches and parses the proxy table from free-proxy-list.net
pub async fn fetch_proxies() -> Result<Vec<String>, Box<dyn Error>> {
    let url = "https://free-proxy-list.net/";
    let body = Client::new().get(url).send().await?.text().await?;
    let document = Html::parse_document(&body);
    let row_sel = Selector::parse(".table-responsive tbody tr").unwrap();
    let cell_sel = Selector::parse("td").unwrap();
    let mut proxies = Vec::new();

    for row in document.select(&row_sel) {
        let cells: Vec<_> = row.select(&cell_sel).collect();
        if cells.len() >= 2 {
            let ip = cells[0].inner_html();
            let port = cells[1].inner_html();
            let country = cells[3].inner_html();
            let https = cells[6].inner_html();
            if country == "Canada" && https == "yes" {
                proxies.push(format!("{}:{}", ip.trim(), port.trim()));
            }
        }
    }
    Ok(proxies)
}
