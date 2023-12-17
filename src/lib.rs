use airtable_flows::create_record;

use anyhow;
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use dotenv::dotenv;
use flowsnet_platform_sdk::logger;
use github_flows::{get_octo, GithubLogin};
use octocrab_wasi::models::DateTimeOrU64;
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

    let date = get_yesterday();
    track_forks(&owner, &repo, &date).await;
    track_stargazers(&owner, &repo, &date).await;
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

// fn get_cron_time_with_date() -> String {
//     let now = Local::now();
//     let now_minute = now.minute() + 2;
//     format!(
//         "{:02} {:02} {:02} {:02} *",
//         now_minute,
//         now.hour(),
//         now.day(),
//         now.month(),
//     )
// }
fn get_yesterday() -> NaiveDate {
    let now = Utc::now();
    let one_day_ago = (now - Duration::days(1i64)).date_naive();
    one_day_ago
}

async fn track_forks(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
    use octocrab_wasi::params::repos::forks::Sort;

    let octocrab = get_octo(&GithubLogin::Default);
    let forks = octocrab
        .repos(owner.to_owned(), repo.to_owned())
        .list_forks()
        .sort(Sort::Oldest)
        .page(1u32)
        .per_page(100)
        .send()
        .await?;

    for f in forks {
        let created_date = match f.created_at {
            Some(DateTimeOrU64::DateTime(dt)) => dt.naive_utc().date(),
            Some(DateTimeOrU64::U64(timestamp)) => {
                let naive_date_time = NaiveDateTime::from_timestamp_opt(timestamp as i64, 0)
                    .ok_or_else(|| anyhow::Error::msg("Invalid timestamp"))?;
                naive_date_time.date()
            }
            _ => continue, // Handle the case where created_at is None
        };

        if created_date < *date {
            break;
        }

        if let Some(o) = f.owner {
            let (name, email, twitter) = get_user_data(&o.login).await?;
            upload_airtable(&name, &email, &twitter).await;
        }
    }
    Ok(())
}
async fn track_stargazers(owner: &str, repo: &str, date: &NaiveDate) -> anyhow::Result<()> {
    let octocrab = get_octo(&GithubLogin::Default);

    let page = octocrab
        .repos(owner.to_owned(), repo.to_owned())
        .list_stargazers()
        // .sort(Sort::CreatedAt)
        .per_page(100)
        .send()
        .await?;
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
            upload_airtable(&name, &email, &twitter).await;
        }
    }
    Ok(())
    // let result = octocrab.all_pages(page).await.unwrap();
    // assert_eq!(result.len(), 3);
    // assert_eq!(result[0].owner.as_ref().unwrap().login, login1);
}

/* async fn track_watcher() {
    use octocrab::{params::repos::forks::Sort, Octocrab};

    let octocrab = get_octo(&GithubLogin::Default);
    let owner = env::var("owner").unwrap_or("wasmedge".to_string());
    let repo = env::var("repo").unwrap_or("wasmedge".to_string());

    let page = octocrab
        .repos(owner.to_owned(), repo.to_owned())
        .list_watchers()
        .per_page(100)
        .sort(octocrab::params::stargazers::Sort::CreatedAt)
        .send()
        .await
        .unwrap();
} */

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

    Ok((login, email, twitter_username))
}
