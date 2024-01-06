use anyhow;
use chrono::{Datelike, Timelike, Utc};
use csv::{QuoteStyle, WriterBuilder};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use schedule_flows::{schedule_cron_job, schedule_handler};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env};

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    // schedule_cron_job(String::from("0 11 * * *"), String::from("cron_job_evoked")).await;

    let now = Utc::now();
    let now_minute = now.minute() + 2;
    let cron_time = format!("{:02} {:02} {:02} * *", now_minute, now.hour(), now.day(),);
    schedule_cron_job(cron_time, String::from("cron_job_evoked")).await;
}

#[schedule_handler]
async fn handler(body: Vec<u8>) {
    dotenv().ok();
    logger::init();
    let owner = env::var("owner").unwrap_or("wasmedge".to_string());
    let repo = env::var("repo").unwrap_or("wasmedge".to_string());

    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .quote_style(QuoteStyle::Always)
        .from_writer(vec![]);

    wtr.write_record(&["Name", "Email", "Twitter", "Watching"])
        .expect("Failed to write record");

    if let Ok(found_watchers_map) = get_watchers(&owner, &repo).await {
        let _ = track_forks(&owner, &repo, &found_watchers_map, &mut wtr).await;
        let _ = track_stargazers(&owner, &repo, &found_watchers_map, &mut wtr).await;
    }

    let _ = upload_to_gist(wtr).await;
}

async fn track_forks(
    owner: &str,
    repo: &str,
    found_map: &HashMap<String, (String, String)>,
    wtr: &mut csv::Writer<Vec<u8>>,
) -> anyhow::Result<()> {
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

    let mut after_cursor: Option<String> = None;
    let octocrab = get_octo(&GithubLogin::Default);

    'outer: for _n in 1..99 {
        let query = format!(
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
            after_cursor
                .as_ref()
                .map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );

        let response: GraphQLResponse = octocrab.graphql(&query).await?;
        let repository = response
            .data
            .and_then(|data| data.repository)
            .and_then(|repo| repo.forks);

        if let Some(forks) = repository {
            for edge in forks.edges.unwrap_or_default() {
                if let Some(node) = edge.node {
                    if let Some(owner) = node.owner {
                        let login = owner.login.unwrap_or_default();

                        match found_map.get(&login) {
                            Some((email, twitter)) => {
                                if let Err(err) = wtr.write_record(&[
                                    login,
                                    email.to_string(),
                                    twitter.to_string(),
                                    "Yes".to_string(),
                                ]) {
                                    log::error!("Failed to write record: {:?}", err);
                                }
                            }

                            None => {
                                if let Err(err) = wtr.write_record(&[
                                    login,
                                    owner.email.unwrap_or("".to_string()),
                                    owner.twitterUsername.unwrap_or("".to_string()),
                                    "".to_string(),
                                ]) {
                                    log::error!("Failed to write record: {:?}", err);
                                }
                            }
                        }
                    }
                }
            }
            wtr.flush()?;

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

    Ok(())
}

async fn track_stargazers(
    owner: &str,
    repo: &str,
    found_map: &HashMap<String, (String, String)>,
    wtr: &mut csv::Writer<Vec<u8>>,
) -> anyhow::Result<()> {
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

    #[derive(Serialize, Deserialize, Debug)]
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

    let mut after_cursor: Option<String> = None;
    let octocrab = get_octo(&GithubLogin::Default);

    'outer: for _n in 1..99 {
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
            after_cursor
                .as_ref()
                .map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );

        let response: GraphQLResponse = octocrab.graphql(&query_str).await?;
        let stargazers = response
            .data
            .and_then(|data| data.repository)
            .and_then(|repo| repo.stargazers);

        if let Some(stargazers) = stargazers {
            for edge in stargazers.edges.unwrap_or_default() {
                if let Some(node) = edge.node {
                    let login = node.login.unwrap_or_default();

                    match found_map.get(&login) {
                        Some((email, twitter)) => {
                            if let Err(err) = wtr.write_record(&[
                                login,
                                email.to_string(),
                                twitter.to_string(),
                                "Yes".to_string(),
                            ]) {
                                log::error!("Failed to write record: {:?}", err);
                            }
                        }

                        None => {
                            if let Err(err) = wtr.write_record(&[
                                login,
                                node.email.unwrap_or("".to_string()),
                                node.twitterUsername.unwrap_or("".to_string()),
                                "".to_string(),
                            ]) {
                                log::error!("Failed to write record: {:?}", err);
                            }
                        }
                    }
                }
            }
            wtr.flush()?;

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

    Ok(())
}

async fn get_watchers(
    owner: &str,
    repo: &str,
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
        url: String,
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

    for _n in 1..99 {
        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    watchers(first: 100, after: {}) {{
                        edges {{
                            node {{
                                login
                                url
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
            after_cursor
                .as_ref()
                .map_or("null".to_string(), |c| format!(r#""{}""#, c))
        );

        let response: GraphQLResponse = octocrab.graphql(&query).await?;
        let watchers = response
            .data
            .and_then(|data| data.repository)
            .and_then(|repo| repo.watchers);

        if let Some(watchers) = watchers {
            for edge in watchers.edges.unwrap_or_default() {
                if let Some(node) = edge.node {
                    watchers_map.insert(
                        node.login,
                        (
                            node.email.unwrap_or_default(),
                            node.twitterUsername.unwrap_or_default(),
                        ),
                    );
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

pub async fn upload_to_gist(wtr: csv::Writer<Vec<u8>>) -> anyhow::Result<()> {
    let octocrab = get_octo(&GithubLogin::Default);

    let data = wtr.into_inner()?;
    let formatted_answer = String::from_utf8(data)?;

    let filename = format!("report_{}.csv", Utc::now().format("%d-%m-%Y"));

    let _ = octocrab
        .gists()
        .create()
        .description("Daily Tracking Report")
        .public(false) // set to true if you want the gist to be public
        .file(filename, formatted_answer)
        .send()
        .await;
    Ok(())
}
