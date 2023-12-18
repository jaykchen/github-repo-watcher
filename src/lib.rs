use airtable_flows::create_record;

use anyhow;
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use octocrab_wasi::{models::DateTimeOrU64, Error as OctoError};
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

    if let Err(e) = track_forks(&owner, &repo, &one_day_ago).await {
        log::error!("Failed to track forks: {:?}", e);
    }
    if let Err(e) = track_stargazers(&owner, &repo, &one_day_ago).await {
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
        data: RepositoryData,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct RepositoryData {
        repository: Repository,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Repository {
        forks: ForkConnection,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ForkConnection {
        edges: Vec<Edge>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Edge {
        node: ForkNode,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ForkNode {
        id: String,
        name: String,
        owner: Owner,
        #[serde(rename = "createdAt")]
        created_at: String, // You can use chrono::DateTime<Utc> for date-time handling
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Owner {
        login: String,
    }

    let octocrab = get_octo(&GithubLogin::Default);
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

    let response: Result<GraphQLResponse, OctoError> = octocrab.graphql(&query).await;

    match response {
        Ok(data) => {
            for edge in data.data.repository.forks.edges {
                let fork_created_at = DateTime::parse_from_rfc3339(&edge.node.created_at)?;
                let fork_date = fork_created_at.naive_utc().date();
                log::info!("{} {}", &fork_created_at, &fork_date);

                if fork_date >= *date {
                    let login = edge.node.owner.login.clone();
                    let (name, email, twitter) = get_user_data(&login).await?;
                    log::info!("{} {} {}", name, email, twitter);
                    upload_airtable(&name, &email, &twitter).await;
                }
            }
        }
        Err(e) => {
            // Handle the error from octocrab
            eprintln!("Error querying GitHub GraphQL API: {}", e);
            return Err(anyhow::Error::new(e).context("Failed to query GitHub GraphQL API"));
        }
    }

    // for f in forks {
    //     let f_clone = f.clone(); // Clone the forks before debugging
    //     dbg!(&f_clone); // Debug the cloned forks

    //     // Serialize the clone to JSON, then take the first 300 characters
    //     let json = serde_json::to_string(&f_clone)?;
    //     let head = json.chars().take(300).collect::<String>();
    //     log::info!("{}", head);

    //     let created_date = match f.created_at {
    //         Some(DateTimeOrU64::DateTime(dt)) => dt.naive_utc().date(),
    //         Some(DateTimeOrU64::U64(timestamp)) => {
    //             let naive_date_time = NaiveDateTime::from_timestamp_opt(timestamp as i64, 0)
    //                 .ok_or_else(|| anyhow::Error::msg("Invalid timestamp"))?;
    //             naive_date_time.date()
    //         }
    //         _ => continue, // Handle the case where created_at is None
    //     };

    //     if created_date < *date {
    //         break;
    //     }

    //     if let Some(o) = f.owner {
    //         let (name, email, twitter) = get_user_data(&o.login).await?;
    //         log::info!("{} {} {}", name, email, twitter);
    //         upload_airtable(&name, &email, &twitter).await;
    //     }
    // }
    Ok(())
}
async fn track_stargazers(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
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
}

pub async fn get_user_data(user: &str) -> anyhow::Result<(String, String, String)> {
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
}
