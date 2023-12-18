use airtable_flows::create_record;

use anyhow;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
// use github_flows::{get_octo, GithubLogin};
// use octocrab_wasi::{models::DateTimeOrU64, Error as OctoError};
use http_req::{request::Method, request::Request, response, uri::Uri};
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
    let one_day_ago = (now - Duration::days(n_days_ago)).date_naive();
    let github_token = env::var("github_token").expect("Env var GITHUB_TOKEN is missing");

    if let Err(e) = track_forks(&github_token, &owner, &repo, &one_day_ago).await {
        log::error!("Failed to track forks: {:?}", e);
    }
    // if let Err(e) = track_stargazers(&owner, &repo, &one_day_ago).await {
    //     log::error!("Failed to track stargazers: {:?}", e);
    // }
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

async fn track_forks(
    github_token: &str,
    owner: &str,
    repo: &str,
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
        created_at: Option<String>, // You can use chrono::DateTime<Utc> for date-time handling
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Owner {
        login: Option<String>,
    }

    let first: i32 = 3; // Replace with the actual number of forks to retrieve

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

    let base_url = "https://api.github.com/graphql";

    let base_url = match Uri::try_from(base_url) {
        Ok(url) => url,
        Err(_) => return Err(anyhow::Error::msg("Invalid base URL")),
    };
    let res = github_http_post(github_token, &base_url.to_string(), &query)
        .await
        .ok_or_else(|| anyhow::Error::msg("Failed to send request or received error response"))?;

    let response: GraphQLResponse = serde_json::from_slice(&res)?;

    if let Some(edges) = response
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
                        log::info!("{} ", login);
                        // Use `?` to propagate potential errors from `get_user_data` and `upload_airtable`.
                        // let (name, email, twitter) = get_user_data(&login).await?;
                        // log::info!("{} {} {}", name, email, twitter);
                        // upload_airtable(&name, &email, &twitter).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn github_http_post(token: &str, base_url: &str, query: &str) -> Option<Vec<u8>> {
    let base_url = Uri::try_from(base_url).unwrap();
    let mut writer = Vec::new();

    let query = serde_json::json!({"query": query});
    match Request::new(&base_url)
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
                log::debug!("Raw HTTP response body: {:?}", String::from_utf8_lossy(&writer));
           
                return None;
            };
            Some(writer)
        }
        Err(_e) => {
            log::error!("Error getting response from Github: {:?}", _e);
            None
        }
    }
}
/* async fn track_stargazers(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
    let octocrab = get_octo(&GithubLogin::Default);

    let page = octocrab
        .repos(owner, repo)
        .list_stargazers()
        // .sort(Sort::CreatedAt)
        .per_page(35)
        .send()
        .await;

    let page = match page {
        Ok(f) => f,
        Err(e) => {
            let error_message =
                format!("Failed to list stargazers for {}/{}: {:?}", owner, repo, e);
            log::error!("{}", &error_message);
            return Err(anyhow::Error::new(e).context(error_message));
        }
    };
    for f in page {
        let created_date = match f.starred_at {
            Some(dt) => dt.naive_utc().date(),
            _ => continue, // Handle the case where starred_at is None
        };

        if created_date < *date {
            break;
        }
        if let Some(u) = f.user {
            let (name, email, twitter) = get_user_data(&u.login).await?;
            log::info!("{} {} {}", name, email, twitter);

            upload_airtable(&name, &email, &twitter).await;
        }
    }
    Ok(())
} */

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

    let profile: UserProfile = octocrab.get(user_profile_url.as_str(), None::<&()>).await?;
    let login = profile.login;
    let email = profile.email.unwrap_or("no email".to_string());
    let twitter_username = profile.twitter_username.unwrap_or("no twitter".to_string());
    log::info!("{} {} {}", login, email, twitter_username);

    Ok((login, email, twitter_username))
} */
