use airtable_flows::create_record;
use anyhow;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
// use octocrab_wasi::{models::DateTimeOrU64, Error as OctoError};
use schedule_flows::{schedule_cron_job, schedule_handler};
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;

#[no_mangle]
#[tokio::main(flavor = "current_thread")]
pub async fn on_deploy() {
    let cron_time = get_cron_time_with_date();
    schedule_cron_job(cron_time, String::from("cron_job_evoked")).await;
    // schedule_cron_job(String::from("0 23 * * *"), String::from("cron_job_evoked")).await;
}

#[schedule_handler]
async fn handler(body: Vec<u8>) {
    dotenv().ok();
    logger::init();
    let owner = env::var("owner").unwrap_or("wasmedge".to_string());
    let repo = env::var("repo").unwrap_or("wasmedge".to_string());
    let n_days_ago = env::var("n_days_ago").unwrap_or("7".to_string());

    let n_days_ago = n_days_ago.parse::<i64>().unwrap_or(7);
    let now = Utc::now();
    let n_days_ago = (now - Duration::days(n_days_ago)).date_naive();

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

fn get_cron_time_with_date() -> String {
    let now = Utc::now();
    let now_minute = now.minute() + 2;
    format!(
        "{:02} {:02} {:02} {:02} *",
        now_minute,
        now.hour(),
        now.day(),
        now.month(),
    )
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

    let first: i32 = 50;

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
                            let fork_date_str = fork_date.format("%Y-%m-%d").to_string();
                            let date_str = date.format("%Y-%m-%d").to_string();
                            log::info!(
                                "forker login: {}, createdAt: {} vs date: {}",
                                login,
                                fork_date_str,
                                date_str
                            );

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
        Err(_e) => {
            log::error!("Failed to query GitHub GraphQL API on forkers: {:?}", _e);
            return Err(anyhow::anyhow!(
                "Failed to query GitHub GraphQL API on forkers"
            ));
        }
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

    let first: i32 = 50; // You can adjust this to your preferred page size

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
                            let starred_date_str = stargazer_date.format("%Y-%m-%d").to_string();
                            let date_str = date.format("%Y-%m-%d").to_string();
                            log::info!(
                                "star giver login: {}, createdAt: {} vs date: {}",
                                login,
                                starred_date_str,
                                date_str
                            );

                            if stargazer_date >= *date {
                                log::info!("{} starred at {}", login, &starred_at_str);
                                let (name, email, twitter) = get_user_data(&login).await?;
                                log::info!("{} {} {}", name, email, twitter);
                                upload_airtable(&name, &email, &twitter).await;
                            }
                        }
                    }
                }
            }
        }
        Err(_e) => {
            log::error!("Failed to query GitHub GraphQL API on stargazers: {:?}", _e);
            return Err(anyhow::anyhow!(
                "Failed to query GitHub GraphQL API on stargazers"
            ));
        }
    }

    Ok(())
}

/* pub async fn get_user_data(user: &str) -> anyhow::Result<(String, String, String)> {
    #[derive(Serialize, Deserialize, Debug)]
    struct UserProfile {
        login: String,
        company: Option<String>,
        blog: Option<String>,
        location: Option<String>,
        email: Option<String>,
        twitter_username: Option<String>,
    }

    // let user_profile_url = format!("https://api.github.com/users/{user}");
    let octocrab = get_octo(&GithubLogin::Default);

    let user_profile_url = format!("/users/{}", user);

    let response = octocrab
        .get::<String, _, ()>(user_profile_url, None::<&()>)
        .await;
    match response {
        Ok(json_string) => {
            // Log or print the raw JSON string for inspection
            log::debug!("Raw JSON string: {}", json_string);

            // Now attempt to deserialize the JSON string into UserProfile
            let profile: UserProfile = serde_json::from_str(&json_string)?;

            let login = profile.login;
            let email = profile.email.unwrap_or_else(|| "no email".to_string());
            let twitter_username = profile
                .twitter_username
                .unwrap_or_else(|| "no twitter".to_string());
            log::info!(
                "Login: {}, Email: {}, Twitter: {}",
                login,
                email,
                twitter_username
            );

            Ok((login, email, twitter_username))
        }
        Err(e) => {
            log::error!("Failed to get user profile: {:?}", e);
            Err(anyhow::anyhow!("Failed to query GitHub user profile API"))
        }
    }
} */

// use github_flows::octocrab::Octocrab;

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

    // let github_token = env::var("github_token").expect("GITHUB_TOKEN not set");

    // let octocrab = Octocrab::builder().personal_token(github_token).build()?;
    let octocrab = get_octo(&GithubLogin::Default);
    let user_profile_url = format!("users/{}", username);

    match octocrab
        .get::<UserProfile, _, ()>(&user_profile_url, None::<&()>)
        .await
    {
        Ok(profile) => {
            let login = profile.login;
            let email = profile.email.unwrap_or_else(|| "no email".to_string());
            let twitter_username = profile
                .twitter_username
                .unwrap_or_else(|| "no twitter".to_string());
            log::info!(
                "Login: {}, Email: {}, Twitter: {}",
                login,
                email,
                twitter_username
            );

            Ok((login, email, twitter_username))
        }
        Err(e) => {
            // Handle the error, e.g., by logging or converting it into an application-specific error.
            log::error!("Failed to get user profile for {}: {:?}", username, e);

            Err(e.into())
        }
    }
}
