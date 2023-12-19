use airtable_flows::create_record;
use anyhow;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use schedule_flows::{schedule_cron_job, schedule_handler};
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    schedule_cron_job(String::from("0 23 * * *"), String::from("cron_job_evoked")).await;
}

#[schedule_handler]
async fn handler(body: Vec<u8>) {
    dotenv().ok();
    logger::init();
    let owner = env::var("owner").unwrap_or("wasmedge".to_string());
    let repo = env::var("repo").unwrap_or("wasmedge".to_string());

    let now = Utc::now();
    let n_days_ago = (now - Duration::days(1)).date_naive();

    if let Err(e) = track_forks(&owner, &repo, &n_days_ago).await {
        log::error!("Failed to track forks: {:?}", e);
    }
    if let Err(e) = track_stargazers(&owner, &repo, &n_days_ago).await {
        log::error!("Failed to track stargazers: {:?}", e);
    }
}

pub async fn upload_airtable(name: &str, email: &str, twitter_username: &str) {
    let airtable_token_name = env::var("airtable_token_name").unwrap_or("github".to_string());
    let airtable_base_id = env::var("airtable_base_id").unwrap_or("appmhvMGsMRPmuUWJ".to_string());
    let airtable_table_name = env::var("airtable_table_name").unwrap_or("mention".to_string());

    let data = serde_json::json!({
        "Name": name,
        "Email": email,
        "Twitter": twitter_username,
    });
    let _ = create_record(
        &airtable_token_name,
        &airtable_base_id,
        &airtable_table_name,
        data.clone(),
    );
}

async fn track_forks(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
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
    struct ForkConnection {
        edges: Option<Vec<Edge>>,
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

    let query = format!(
        r#"
        query {{
            repository(owner: "{}", name: "{}") {{
                forks(first: {}, orderBy: {{field: CREATED_AT, direction: DESC}}) {{
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
                }}
            }}
        }}
        "#,
        owner, repo, first
    );

    let octocrab = get_octo(&GithubLogin::Default);

    match octocrab.graphql::<GraphQLResponse>(&query).await {
        Ok(data) => {
            if let Some(edges) = data
                .data
                .and_then(|d| d.repository.and_then(|r| r.forks.and_then(|f| f.edges)))
            {
                for edge in edges {
                    if let Some(node) = edge.node {
                        if let (Some(login), Some(created_at_str)) =
                            (node.owner.and_then(|o| o.login), node.created_at)
                        {
                            let fork_created_at = DateTime::parse_from_rfc3339(&created_at_str)?;
                            let fork_date = fork_created_at.naive_utc().date();

                            if fork_date >= *date {
                                let (name, email, twitter) = get_user_data(&login).await?;
                                log::info!("{} {} {}", name, email, twitter);
                                upload_airtable(&name, &email, &twitter).await;
                            }
                        }
                    }
                }
            }
        }
        Err(_e) => log::error!("Failed to query GitHub GraphQL API on forkers: {:?}", _e),
    }

    Ok(())
}

async fn track_stargazers(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
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
    struct StargazerConnection {
        edges: Option<Vec<StargazerEdge>>,
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

    let first: i32 = 100; // You can adjust this to your preferred page size

    let query = format!(
        r#"
        query {{
            repository(owner: "{}", name: "{}") {{
                stargazers(first: {}, orderBy: {{field: STARRED_AT, direction: DESC}}) {{
                    edges {{
                        node {{
                            id
                            login
                        }}
                        starredAt
                    }}
                }}
            }}
        }}
        "#,
        owner, repo, first
    );

    let octocrab = get_octo(&GithubLogin::Default);

    match octocrab.graphql::<GraphQLResponse>(&query).await {
        Ok(data) => {
            if let Some(edges) = data.data.and_then(|d| {
                d.repository
                    .and_then(|r| r.stargazers.and_then(|f| f.edges))
            }) {
                for edge in edges {
                    if let (Some(node), Some(ref starred_at_str)) = (edge.node, edge.starred_at) {
                        if let Some(ref login) = node.login {
                            let stargazer_starred_at =
                                DateTime::parse_from_rfc3339(&starred_at_str)?;
                            let stargazer_date = stargazer_starred_at.naive_utc().date();

                            if stargazer_date >= *date {
                                let (name, email, twitter) = get_user_data(&login).await?;
                                log::info!("{} {} {}", name, email, twitter);
                                upload_airtable(&name, &email, &twitter).await;
                            }
                        }
                    }
                }
            }
        }
        Err(_e) => log::error!("Failed to query GitHub GraphQL API on stargazers: {:?}", _e),
    }

    Ok(())
}

async fn get_user_data(username: &str) -> anyhow::Result<(String, String, String)> {
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
    let user_profile_url = format!("users/{}", username);

    match octocrab
        .get::<UserProfile, _, ()>(&user_profile_url, None::<&()>)
        .await
    {
        Ok(profile) => {
            let login = profile.login;
            let email = profile.email.unwrap_or("no email".to_string());
            let twitter_username = profile.twitter_username.unwrap_or("no twitter".to_string());

            Ok((login, email, twitter_username))
        }
        Err(e) => {
            log::error!("Failed to get user info for {}: {:?}", username, e);
            Err(e.into())
        }
    }
}
