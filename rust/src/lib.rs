use pyo3::prelude::*;
use pyo3_asyncio::tokio;

mod crawler;
mod proxy_list;
mod validator;

#[pymodule]
fn rust_proxy_crawler(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_proxies, m)?)?;
    m.add_function(wrap_pyfunction!(get_proxies_async, m)?)?;
    m.add_function(wrap_pyfunction!(validate_proxy_async, m)?)?;
    m.add_function(wrap_pyfunction!(crawl_url_async, m)?)?;
    Ok(())
}

#[pyfunction]
fn get_proxies() -> PyResult<Vec<String>> {
    let rt = tokio::runtime()?;
    rt.block_on(proxy_list::fetch_proxies())
}

#[pyfunction]
fn get_proxies_async(py: Python) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move { proxy_list::fetch_proxies().await })
}

#[pyfunction]
fn validate_proxy_async(py: Python, proxy: String, target_url: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        validator::validate(&proxy, &target_url).await
    })
}

#[pyfunction]
fn crawl_url_async(py: Python, url: String, proxy: Option<String>) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(
        py,
        async move { crawler::crawl_with_proxy(&url, proxy).await },
    )
}
