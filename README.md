# github-repo-watcher

A simple tool to watch a Github repository and logs new users that engage by creating new forks, starring the repository, or watch it. The daily log is saved to the your gist as a private gist in csv format.


## Usage

Set the exact time to trigger the serverless function as a cron job in this code block.


```
schedule_cron_job(String::from("0 11 * * *"), String::from("cron_job_evoked")).await;
```

Set the 'owner' and 'repo' environment variables on the Flows settings page.


Then, you'll get daily updates on your gist.
