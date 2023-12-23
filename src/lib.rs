use airtable_flows::create_record;
use anyhow;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use schedule_flows::{schedule_cron_job, schedule_handler};
use serde::{Deserialize, Serialize};
use serde_json;
use std::{collections::HashSet, env};

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    // schedule_cron_job(String::from("0 11 * * *"), String::from("cron_job_evoked")).await;

    let now = Utc::now();
    let now_minute = now.minute() + 2;
    let cron_time = format!(
        "{:02} {:02} {:02} {:02} *",
        now_minute,
        now.hour(),
        now.day(),
        now.month(),
    );
    schedule_cron_job(cron_time, String::from("cron_job_evoked")).await;
}

#[schedule_handler]
async fn handler(body: Vec<u8>) {
    dotenv().ok();
    logger::init();
    let owner = env::var("owner").unwrap_or("wasmedge".to_string());
    let repo = env::var("repo").unwrap_or("wasmedge".to_string());

    let now = Utc::now();
    let n_days_ago = (now - Duration::days(7)).date_naive();

    match get_watchers(&owner, &repo).await {
        Ok(watchers) => {
            if let Err(e) = track_forks(&owner, &repo, &watchers, &n_days_ago).await {
                log::error!("Failed to track forks: {:?}", e);
            }
            if let Err(e) = track_stargazers(&owner, &repo, &watchers, &n_days_ago).await {
                log::error!("Failed to track stargazers: {:?}", e);
            }
        }

        Err(e) => log::error!("Failed to get watchers: {:?}", e),
    }
}

pub async fn upload_airtable(login: &str, email: &str, twitter_username: &str, watching: bool) {
    let airtable_token_name = env::var("airtable_token_name").unwrap_or("github".to_string());
    let airtable_base_id = env::var("airtable_base_id").unwrap_or("appmhvMGsMRPmuUWJ".to_string());
    let airtable_table_name = env::var("airtable_table_name").unwrap_or("mention".to_string());

    let data = serde_json::json!({
        "Name": login,
        "Email": email,
        "Twitter": twitter_username,
        "Watching": watching,
    });
    let _ = create_record(
        &airtable_token_name,
        &airtable_base_id,
        &airtable_table_name,
        data.clone(),
    );
}

async fn track_forks(
    owner: &str,
    repo: &str,
    watchers_set: &HashSet<String>,
    date: &NaiveDate,
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
        #[serde(rename = "createdAt")]
        created_at: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Owner {
        login: Option<String>,
    }

    let first: i32 = 100;

    #[derive(Serialize, Deserialize, Debug)]
    struct PageInfo {
        end_cursor: Option<String>,
        has_next_page: bool,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ForkConnection {
        edges: Option<Vec<Edge>>,
        page_info: Option<PageInfo>, // Add this field to your ForkConnection struct
    }

    let first: i32 = 100;
    let mut after_cursor: Option<String> = None;

    let octocrab = get_octo(&GithubLogin::Default);
let mut count = 0;
    loop {
        count += 1;
        log::info!("fork loop {}", count);

        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    forks(first: {}, after: {}, orderBy: {{field: CREATED_AT, direction: DESC}}) {{
                        edges {{
                            node {{
                                id
                                name
                                owner {{
                                    login
                                }}
                                createdAt
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
            first,
            after_cursor
                .as_ref()
                .map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );
        log::info!("query {}", query);

        let response: GraphQLResponse = octocrab.graphql(&query).await?;
        if let Some(repository_data) = response.data {
            if let Some(repository) = repository_data.repository {
                if let Some(forks) = repository.forks {
                    if let Some(edges) = forks.edges {
                        for edge in edges {
                            if let Some(node) = edge.node {
                                if let (Some(login), Some(created_at_str)) =
                                    (node.owner.and_then(|o| o.login), node.created_at)
                                {
                                    let fork_created_at =
                                        DateTime::parse_from_rfc3339(&created_at_str)?;
                                    let fork_date = fork_created_at.naive_utc().date();

                                    if fork_date >= *date {
                                        let (email, twitter) = get_user_data(&login).await?;
                                        // log::info!("{} {} {}", &login, email, twitter);
                                        let is_watching = watchers_set.contains(&login);
                                        upload_airtable(&login, &email, &twitter, is_watching)
                                            .await;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(page_info) = forks.page_info {
                        after_cursor = page_info.end_cursor;
                        if !page_info.has_next_page {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
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
    watchers_set: &HashSet<String>,
    date: &NaiveDate,
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
        #[serde(rename = "starredAt")]
        starred_at: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct StargazerNode {
        id: Option<String>,
        login: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct PageInfo {
        end_cursor: Option<String>,
        has_next_page: bool,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct StargazerConnection {
        edges: Option<Vec<StargazerEdge>>,
        page_info: Option<PageInfo>,
    }

    let first: i32 = 100;
    let mut after_cursor: Option<String> = None;

    let octocrab = get_octo(&GithubLogin::Default);
let mut count = 0;
    loop {
        count += 1;
        log::info!("stars loop {}", count);
        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    stargazers(first: {}, after: {}, orderBy: {{field: STARRED_AT, direction: DESC}}) {{
                        edges {{
                            node {{
                                id
                                login
                            }}
                            starredAt
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
            first,
            after_cursor
                .as_ref()
                .map_or("null".to_string(), |cursor| format!(r#""{}""#, cursor))
        );
        log::info!("query {}", query);

        let response: GraphQLResponse = octocrab.graphql(&query).await?;
        if let Some(repository_data) = response.data {
            if let Some(repository) = repository_data.repository {
                if let Some(stargazers) = repository.stargazers {
                    if let Some(edges) = stargazers.edges {
                        for edge in edges {
                            if let (Some(node), Some(starred_at_str)) = (edge.node, edge.starred_at)
                            {
                                if let Some(login) = node.login {
                                    let stargazer_starred_at =
                                        DateTime::parse_from_rfc3339(&starred_at_str)?;
                                    let stargazer_date = stargazer_starred_at.naive_utc().date();

                                    if stargazer_date >= *date {
                                        let (email, twitter) = get_user_data(&login).await?;
                                        // log::info!("{} {} {}", &login, email, twitter);
                                        let is_watching = watchers_set.contains(&login);
                                        upload_airtable(&login, &email, &twitter, is_watching)
                                            .await;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(page_info) = stargazers.page_info {
                        after_cursor = page_info.end_cursor;
                        if !page_info.has_next_page {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
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

async fn get_user_data(login: &str) -> anyhow::Result<(String, String)> {
    #[derive(Serialize, Deserialize, Debug)]
    struct UserProfile {
        login: String,
        company: Option<String>,
        blog: Option<String>,
        location: Option<String>,
        email: Option<String>,
        twitter_username: Option<String>,
    }

    let octocrab = get_octo(&GithubLogin::Default);
    let user_profile_url = format!("users/{}", login);

    match octocrab
        .get::<UserProfile, _, ()>(&user_profile_url, None::<&()>)
        .await
    {
        Ok(profile) => {
            let email = profile.email.unwrap_or("no email".to_string());
            let twitter_username = profile.twitter_username.unwrap_or("no twitter".to_string());

            Ok((email, twitter_username))
        }
        Err(e) => {
            log::error!("Failed to get user info for {}: {:?}", login, e);
            Err(e.into())
        }
    }
}

async fn get_watchers(owner: &str, repo: &str) -> anyhow::Result<HashSet<String>> {
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
        #[serde(rename = "createdAt")]
        created_at: String,
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

    let mut watchers_set = HashSet::<String>::new();
    let octocrab = get_octo(&GithubLogin::Default);
    let mut before_cursor = None;
let mut count = 0;
    loop {
        count += 1;
        log::info!("watcher loop {}", count);

        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    watchers(last: 100, before: {}) {{
                        edges {{
                            node {{
                                login
                                url
                                createdAt
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
            before_cursor
                .as_ref()
                .map_or("null".to_string(), |c| format!(r#""{}""#, c))
        );
        log::info!("query {}", query);
        let response: GraphQLResponse = octocrab.graphql(&query).await?;
        if let Some(data) = response.data {
            if let Some(repository) = data.repository {
                if let Some(watchers) = repository.watchers {
                    if let Some(edges) = watchers.edges {
                        for edge in edges {
                            if let Some(node) = edge.node {
                                watchers_set.insert(node.login);
                            }
                            if watchers_set.len() >= 1000 {
                                break;
                            }
                        }
                    }
                    if let Some(page_info) = watchers.page_info {
                        if page_info.has_next_page.unwrap_or(false) {
                            before_cursor = page_info.end_cursor;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
    if watchers_set.len() > 0 {
        Ok(watchers_set)
    } else {
        Err(anyhow::anyhow!("no watchers"))
    }
}
