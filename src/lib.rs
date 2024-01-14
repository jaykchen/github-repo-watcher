use anyhow;
use csv::{ QuoteStyle, WriterBuilder };
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{ get_octo, GithubLogin };
use serde::{ Deserialize, Serialize };
use std::{ collections::HashMap, env };
use serde_json::Value;
use webhook_flows::{ create_endpoint, request_handler, send_response };

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    create_endpoint().await;
}

#[request_handler]
async fn handler(
    _headers: Vec<(String, String)>,
    _subpath: String,
    _qry: HashMap<String, Value>,
    _body: Vec<u8>
) {
    dotenv().ok();
    logger::init();

    let token = env::var("GITHUB_TOKEN").expect("github_token is required");

    let (owner, repo) = match
        (
            _qry.get("owner").unwrap_or(&Value::Null).as_str(),
            _qry.get("repo").unwrap_or(&Value::Null).as_str(),
        )
    {
        (Some(o), Some(r)) => (o.to_string(), r.to_string()),
        (_, _) => {
            send_response(
                400,
                vec![(String::from("content-type"), String::from("text/plain"))],
                "You must provide an owner and repo name.".as_bytes().to_vec()
            );
            return;
        }
    };

    let mut watchers_map = get_watchers(&owner, &repo).await.unwrap_or_default();
    let mut forked_map = track_forks(&owner, &repo).await.unwrap_or_default();
    let mut starred_map = track_stargazers(&owner, &repo).await.unwrap_or_default();

    match report_as_md(&mut watchers_map, &mut forked_map, &mut starred_map).await {
        Err(_e) => {
            log::error!("Error generating report in md: {:?}", _e);
            send_response(
                400,
                vec![(String::from("content-type"), String::from("text/plain"))],
                "You've entered invalid owner/repo, or the target is private. Please try again."
                    .as_bytes()
                    .to_vec()
            );
            std::process::exit(1);
        }
        Ok(report) => {
            send_response(
                200,
                vec![(String::from("content-type"), String::from("text/plain"))],
                report.as_bytes().to_vec()
            );
        }
    }
    return;
}

async fn track_forks(owner: &str, repo: &str) -> anyhow::Result<HashMap<String, (String, String)>> {
    #[derive(Serialize, Deserialize, Debug)]
    struct GraphQLResponse {
        data: Option<RepositoryData>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct RepositoryData {
        repository: Option<Repository>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Repository {
        forks: Option<ForkConnection>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Edge {
        node: Option<ForkNode>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ForkNode {
        id: Option<String>,
        name: Option<String>,
        owner: Option<Owner>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Owner {
        login: Option<String>,
        email: Option<String>,
        twitterUsername: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct PageInfo {
        #[serde(rename = "endCursor")]
        end_cursor: Option<String>,
        #[serde(rename = "hasNextPage")]
        has_next_page: Option<bool>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ForkConnection {
        edges: Option<Vec<Edge>>,
        #[serde(rename = "pageInfo")]
        page_info: Option<PageInfo>,
    }
    let mut forked_map: HashMap<String, (String, String)> = HashMap::new();
    let mut after_cursor: Option<String> = None;

    for _n in 1..499 {
        let query_str = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    forks(first: 100, after: {}) {{
                        edges {{
                            node {{
                                id
                                name
                                owner {{
                                    login
                                    ... on User {{
                                        email
                                        twitterUsername
                                    }}
                                }}
                            }}
                        }}
                        pageInfo {{
                            endCursor
                            hasNextPage
                        }}
                    }}
                }}
            }}
            "#,
            owner,
            repo,
            after_cursor.as_ref().map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );

        let response: GraphQLResponse;
        // let mut response: GraphQLResponse = GraphQLResponse { data: None };

        match github_http_post_gql(&query_str).await {
            Ok(r) => {
                response = match serde_json::from_slice::<GraphQLResponse>(&r) {
                    Ok(res) => res,
                    Err(err) => {
                        log::error!("Failed to deserialize response from Github: {}", err);
                        continue;
                    }
                };
            }
            Err(_e) => {
                continue;
            }
        }

        let repository = response.data.and_then(|data| data.repository).and_then(|repo| repo.forks);

        if let Some(forks) = repository {
            for edge in forks.edges.unwrap_or_default() {
                if let Some(owner) = edge.node.and_then(|node| node.owner) {
                    forked_map.insert(owner.login.unwrap_or_default(), (
                        owner.email.unwrap_or("".to_string()),
                        owner.twitterUsername.unwrap_or("".to_string()),
                    ));
                }
            }

            if let Some(page_info) = forks.page_info {
                if page_info.has_next_page.unwrap_or(false) {
                    after_cursor = page_info.end_cursor;
                } else {
                    log::info!("fork loop {}", _n);
                    break;
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(forked_map)
}

async fn track_stargazers(
    owner: &str,
    repo: &str
) -> anyhow::Result<HashMap<String, (String, String)>> {
    #[derive(Serialize, Deserialize, Debug)]
    struct GraphQLResponse {
        data: Option<RepositoryData>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct RepositoryData {
        repository: Option<Repository>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Repository {
        stargazers: Option<StargazerConnection>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct StargazerEdge {
        node: Option<StargazerNode>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct StargazerNode {
        id: Option<String>,
        login: Option<String>,
        email: Option<String>,
        twitterUsername: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct PageInfo {
        #[serde(rename = "endCursor")]
        end_cursor: Option<String>,
        #[serde(rename = "hasNextPage")]
        has_next_page: Option<bool>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct StargazerConnection {
        edges: Option<Vec<StargazerEdge>>,
        #[serde(rename = "pageInfo")]
        page_info: Option<PageInfo>,
    }

    let mut starred_map: HashMap<String, (String, String)> = HashMap::new();

    let mut after_cursor: Option<String> = None;

    for _n in 1..499 {
        let query_str = format!(
            r#"query {{
                repository(owner: "{}", name: "{}") {{
                    stargazers(first: 100, after: {}) {{
                        edges {{
                            node {{
                                id
                                login
                                email
                                twitterUsername
                            }}
                        }}
                        pageInfo {{
                            endCursor
                            hasNextPage
                        }}
                    }}
                }}
            }}"#,
            owner,
            repo,
            after_cursor.as_ref().map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );

        let response: GraphQLResponse;

        match github_http_post_gql(&query_str).await {
            Ok(r) => {
                response = match serde_json::from_slice::<GraphQLResponse>(&r) {
                    Ok(res) => res,
                    Err(err) => {
                        log::error!("Failed to deserialize response from Github: {}", err);
                        continue;
                    }
                };
            }
            Err(_e) => {
                continue;
            }
        }
        let stargazers = response.data
            .and_then(|data| data.repository)
            .and_then(|repo| repo.stargazers);

        if let Some(stargazers) = stargazers {
            for edge in stargazers.edges.unwrap_or_default() {
                if let Some(node) = edge.node {
                    starred_map.insert(node.login.clone().unwrap_or_default(), (
                        node.email.unwrap_or(String::from("")),
                        node.twitterUsername.unwrap_or(String::from("")),
                    ));
                }
            }

            if let Some(page_info) = stargazers.page_info {
                if page_info.has_next_page.unwrap_or(false) {
                    after_cursor = page_info.end_cursor;
                } else {
                    log::info!("stargazers loop {}", _n);
                    break;
                }
            } else {
                log::error!("pageInfo is missing from the response");
                break;
            }
        } else {
            break;
        }
    }

    Ok(starred_map)
}

async fn get_watchers(
    owner: &str,
    repo: &str
) -> anyhow::Result<HashMap<String, (String, String)>> {
    #[derive(Serialize, Deserialize, Debug)]
    struct GraphQLResponse {
        data: Option<RepositoryData>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct RepositoryData {
        repository: Option<Repository>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Repository {
        watchers: Option<WatchersConnection>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct WatcherEdge {
        node: Option<WatcherNode>,
    }
    #[derive(Serialize, Deserialize, Debug)]
    struct WatcherNode {
        login: String,
        email: Option<String>,
        twitterUsername: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct PageInfo {
        #[serde(rename = "endCursor")]
        end_cursor: Option<String>,
        #[serde(rename = "hasNextPage")]
        has_next_page: Option<bool>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct WatchersConnection {
        edges: Option<Vec<WatcherEdge>>,
        #[serde(rename = "pageInfo")]
        page_info: Option<PageInfo>,
    }

    let mut watchers_map = HashMap::<String, (String, String)>::new();

    let octocrab = get_octo(&GithubLogin::Default);
    let mut after_cursor = None;

    for _n in 1..499 {
        let query_str = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    watchers(first: 100, after: {}) {{
                        edges {{
                            node {{
                                login
                                email
                                twitterUsername
                            }}
                        }}
                        pageInfo {{
                            endCursor
                            hasNextPage
                        }}
                    }}
                }}
            }}
            "#,
            owner,
            repo,
            after_cursor.as_ref().map_or("null".to_string(), |c| format!(r#""{}""#, c))
        );

        let response: GraphQLResponse;
        match github_http_post_gql(&query_str).await {
            Ok(r) => {
                response = match serde_json::from_slice::<GraphQLResponse>(&r) {
                    Ok(res) => res,
                    Err(err) => {
                        log::error!("Failed to deserialize response from Github: {}", err);
                        continue;
                    }
                };
            }
            Err(_e) => {
                continue;
            }
        }
        let watchers = response.data
            .and_then(|data| data.repository)
            .and_then(|repo| repo.watchers);

        if let Some(watchers) = watchers {
            for edge in watchers.edges.unwrap_or_default() {
                if let Some(node) = edge.node {
                    watchers_map.insert(node.login, (
                        node.email.unwrap_or(String::from("")),
                        node.twitterUsername.unwrap_or(String::from("")),
                    ));
                }
            }

            match watchers.page_info {
                Some(page_info) if page_info.has_next_page.unwrap_or(false) => {
                    after_cursor = page_info.end_cursor;
                }
                _ => {
                    log::info!("watchers loop {}", _n);
                    break;
                }
            }
        } else {
            break;
        }
    }
    if !watchers_map.is_empty() {
        log::info!("Found {} watchers for {}", watchers_map.len(), repo);
        Ok(watchers_map)
    } else {
        Err(anyhow::anyhow!("no watchers"))
    }
}

pub async fn report_as_md(
    watchers_map: &mut HashMap<String, (String, String)>,
    forked_map: &mut HashMap<String, (String, String)>,
    starred_map: &mut HashMap<String, (String, String)>
) -> anyhow::Result<String> {
    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .quote_style(QuoteStyle::Always)
        .from_writer(vec![]);

    wtr.write_record(&["Name", "Forked", "Starred", "Watching", "Email", "Twitter"]).expect(
        "Failed to write record"
    );
    log::info!("forked_map len {}", forked_map.len());

    for (login, (email, twitter)) in &mut *forked_map {
        let starred_or_not = match starred_map.remove(login) {
            Some(_) => String::from("Y"),
            None => String::from(""),
        };
        let is_watching = match watchers_map.remove(login) {
            Some(_) => String::from("Y"),
            None => String::from(""),
        };

        if
            let Err(err) = wtr.write_record(
                &[login, &String::from("Y"), &starred_or_not, &is_watching, email, twitter]
            )
        {
            log::error!("Failed to write record: {:?}", err);
        }
    }
    log::info!("star_map len {}", starred_map.len());

    for (login, (email, twitter)) in starred_map {
        let forked_or_not = match forked_map.remove(login) {
            Some(_) => String::from("Y"),
            None => String::from(""),
        };

        let is_watching = match watchers_map.remove(login) {
            Some(_) => String::from("Y"),
            None => String::from(""),
        };

        if
            let Err(err) = wtr.write_record(
                &[login, &forked_or_not, &String::from("Y"), &is_watching, email, twitter]
            )
        {
            log::error!("Failed to write record: {:?}", err);
        }
    }
    log::info!("wachers len {}", watchers_map.len());

    for (login, (email, twitter)) in watchers_map {
        if
            let Err(err) = wtr.write_record(
                &[login, &String::from(""), &String::from(""), &String::from("Y"), email, twitter]
            )
        {
            log::error!("Failed to write record: {:?}", err);
        }
    }
    // wtr.flush()?;

    let data = match wtr.into_inner() {
        Ok(d) => d,
        Err(_e) => {
            log::error!("Failed to get inner writer: {:?}", _e);
            vec![]
        }
    };

    let csv_data = String::from_utf8(data)?;

    let mut rdr = csv::Reader::from_reader(csv_data.as_bytes());
    let mut markdown_output = String::new();

    if let Some(headers) = rdr.headers().ok() {
        let headers_md: Vec<String> = headers
            .iter()
            .map(|s| s.to_string())
            .collect();
        markdown_output.push_str(&format!("| {} |\n", headers_md.join(" | ")));
        markdown_output.push_str(
            &format!(
                "|{}|\n",
                headers_md
                    .iter()
                    .map(|_| "---")
                    .collect::<Vec<_>>()
                    .join("|")
            )
        );
    }

    for result in rdr.records() {
        let record = result?;
        let row_md: Vec<String> = record
            .iter()
            .map(|s| s.to_string())
            .collect();
        markdown_output.push_str(&format!("| {} |\n", row_md.join(" | ")));
    }
    Ok(markdown_output)
}

pub async fn github_http_post_gql(query: &str) -> anyhow::Result<Vec<u8>> {
    use http_req::{ request::Method, request::Request, uri::Uri };
    let token = env::var("GITHUB_TOKEN").expect("github_token is required");
    let base_url = "https://api.github.com/graphql";
    let base_url = Uri::try_from(base_url).unwrap();
    let mut writer = Vec::new();

    let query = serde_json::json!({"query": query});
    match
        Request::new(&base_url)
            .method(Method::POST)
            .header("User-Agent", "flows-network connector")
            .header("Content-Type", "application/json")
            .header("Authorization", &format!("Bearer {}", token))
            .header("Content-Length", &query.to_string().len())
            .body(&query.to_string().into_bytes())
            .send(&mut writer)
    {
        Ok(res) => {
            if !res.status_code().is_success() {
                log::error!("Github http error {:?}", res.status_code());
                return Err(anyhow::anyhow!("Github http error {:?}", res.status_code()));
            }
            Ok(writer)
        }
        Err(_e) => {
            log::error!("Error getting response from Github: {:?}", _e);
            Err(anyhow::anyhow!(_e))
        }
    }
}
