# github-repo-watcher

A simple tool to watch a Github repository and logs new users that engage by creating new forks, starring the repository, or watch it. The report is delivered to you in markeddown format via webhook.


## Usage

Set your 'GITHUB_TOKEN' environment variables on the Flows settings page.

You'll get a unique webhook url after your function is deployed.

Use the webhook url to call the function. For example:

```
https://code.flows.network/webhook/vIognrnNfVdQSlIRIyIM?owner=second-state&repo=LlamaEdge
```

