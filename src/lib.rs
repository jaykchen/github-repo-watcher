use anyhow;
use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use csv::{QuoteStyle, WriterBuilder};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use schedule_flows::{schedule_cron_job, schedule_handler};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, env};

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    // schedule_cron_job(String::from("0 11 * * *"), String::from("cron_job_evoked")).await;

    let now = Utc::now();
    let now_minute = now.minute() + 1;
    let cron_time = format!("{:02} {:02} {:02} * *", now_minute, now.hour(), now.day(),);
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

    let mut found_logins_set = HashSet::new();

    let mut wtr = WriterBuilder::new()
        .delimiter(b',')
        .quote_style(QuoteStyle::Always)
        .from_writer(vec![]);

    wtr.write_record(&["Name", "Email", "Twitter", "Watching"])
        .expect("Failed to write record");
    if let Ok(watchers) = get_watchers(&owner, &repo).await {
        let _ = track_forks(
            &owner,
            &repo,
            &watchers,
            &mut found_logins_set,
            &n_days_ago,
            &mut wtr,
        )
        .await;
        let _ = track_stargazers(
            &owner,
            &repo,
            &watchers,
            &mut found_logins_set,
            &n_days_ago,
            &mut wtr,
        )
        .await;
    }

    let _ = upload_to_gist(wtr).await;
}

async fn track_forks(
    owner: &str,
    repo: &str,
    watchers_set: &HashSet<String>,
    found_set: &mut HashSet<String>,
    date: &NaiveDate,
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
        #[serde(rename = "createdAt")]
        created_at: Option<String>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Owner {
        login: Option<String>,
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
    let mut count_out_of_range = 0;

    'outer: for _n in 1..100 {
        // log::info!("fork loop {}", _n);

        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    forks(first: 100, after: {}, orderBy: {{field: CREATED_AT, direction: DESC}}) {{
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
                    if let (Some(login), Some(created_at_str)) =
                        (node.owner.and_then(|o| o.login), node.created_at)
                    {
                        let fork_created_at = DateTime::parse_from_rfc3339(&created_at_str)?;
                        let fork_date = fork_created_at.naive_utc().date();

                        if count_out_of_range > 10 {
                            break 'outer;
                        }

                        if fork_date >= *date {
                            if !found_set.insert(login.clone()) {
                                continue;
                            }
                            let (email, twitter) = get_user_data(&login).await?;

                            let is_watching = match watchers_set.contains(&login) {
                                true => "yes".to_string(),
                                false => "".to_string(),
                            };
                            wtr.write_record(&[login, email, twitter, is_watching])
                                .expect("Failed to write record");
                        } else {
                            count_out_of_range += 1;
                        }
                    }
                }
            }

            if let Some(page_info) = forks.page_info {
                if page_info.has_next_page.unwrap_or(false) {
                    after_cursor = page_info.end_cursor;
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
    found_set: &mut HashSet<String>,
    date: &NaiveDate,
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
    let mut count_out_of_range = 0;

    'outer: for _n in 1..100 {
        // log::info!("stargazers loop {}", _n);

        let query_str = format!(
            r#"query {{
                repository(owner: "{}", name: "{}") {{
                    stargazers(first: 100, after: {}, orderBy: {{field: STARRED_AT, direction: DESC}}) {{
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
                    if let (Some(login), Some(starred_at_str)) = (node.login, edge.starred_at) {
                        match DateTime::parse_from_rfc3339(&starred_at_str) {
                            Ok(stargazer_starred_at) => {
                                let stargazer_date = stargazer_starred_at.naive_utc().date();

                                if count_out_of_range > 10 {
                                    break 'outer;
                                }

                                if stargazer_date >= *date {
                                    if !found_set.insert(login.clone()) {
                                        continue;
                                    }
                                    let (email, twitter) = get_user_data(&login).await?;
                                    let is_watching = match watchers_set.contains(&login) {
                                        true => "yes".to_string(),
                                        false => "".to_string(),
                                    };
                                    wtr.write_record(&[login, email, twitter, is_watching])
                                        .expect("Failed to write record");
                                } else {
                                    count_out_of_range += 1;
                                }
                            }
                            Err(e) => log::error!("Failed to parse star date: {}", e),
                        }
                    }
                }
            }

            if let Some(page_info) = stargazers.page_info {
                if page_info.has_next_page.unwrap_or(false) {
                    after_cursor = page_info.end_cursor;
                } else {
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
    let mut after_cursor = None;

    for _n in 1..50 {
        // log::info!("watchers loop {}", _n);

        let query = format!(
            r#"
            query {{
                repository(owner: "{}", name: "{}") {{
                    watchers(first: 100, after: {}) {{
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
                    watchers_set.insert(node.login);
                }
            }

            match watchers.page_info {
                Some(page_info) if page_info.has_next_page.unwrap_or(false) => {
                    after_cursor = page_info.end_cursor;
                }
                _ => break,
            }
        } else {
            break;
        }
    }
    if !watchers_set.is_empty() {
        Ok(watchers_set)
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

async fn get_user_data(login: &str) -> anyhow::Result<(String, String)> {
    #[derive(Serialize, Deserialize, Debug)]
    struct UserProfile {
        login: String,
        company: Option<String>,
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
            let email = profile.email.unwrap_or("".to_string());
            let twitter_username = profile.twitter_username.unwrap_or("".to_string());

            Ok((email, twitter_username))
        }
        Err(e) => {
            log::error!("Failed to get user info for {}: {:?}", login, e);
            Err(e.into())
        }
    }
}
