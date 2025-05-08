import os
import re
import requests
import pandas as pd
from datetime import datetime
from git import Repo

# -------------------- Configuration --------------------

class ProjectConfig:
    def __init__(self, name, jira_key, repo_url, local_repo_path):
        self.name = name
        self.jira_key = jira_key
        self.repo_url = repo_url
        self.local_repo_path = local_repo_path
        self.issue_key_pattern = rf'({jira_key}-\d+)'
        self.jira_api_url = "https://issues.apache.org/jira/rest/api/2/search"
        self.jira_query = (
            f'project={jira_key} AND statusCategory=Done AND resolution != "Won\'t Fix"'
        )

# -------------------- Components --------------------

class JiraClient:
    def __init__(self, config, max_results=1000):
        self.config = config
        self.max_results = max_results

    def fetch_issues(self):
        issues = []
        start_at = 0
        print("Fetching issues from JIRA...")
        while True:
            print(f"Requesting issues {start_at} to {start_at + self.max_results}...")
            params = {
                'jql': self.config.jira_query,
                'startAt': start_at,
                'maxResults': self.max_results,
                'fields': 'key,summary,issuetype,priority,created,resolutiondate,description,comment',
                'expand': 'changelog'
            }
            response = requests.get(self.config.jira_api_url, params=params)
            response.raise_for_status()
            data = response.json()
            issues.extend(data['issues'])
            if start_at + self.max_results >= data['total']:
                break
            start_at += self.max_results
        print(f"Total issues fetched: {len(issues)}")
        return issues

class GitRepoAnalyzer:
    def __init__(self, config):
        self.config = config
        self.repo = self._open_or_clone_repo()

    def _open_or_clone_repo(self):
        if os.path.exists(self.config.local_repo_path):
            print(f"Using existing local repo at {self.config.local_repo_path}")
            return Repo(self.config.local_repo_path)
        print(f"Cloning repository for {self.config.name}...")
        return Repo.clone_from(self.config.repo_url, self.config.local_repo_path)

    def map_issues_to_commits(self, issues):
        print("Mapping issues to commits...")
        issue_commit_map = {}
        for commit in self.repo.iter_commits():
            matches = re.findall(self.config.issue_key_pattern, commit.message)
            for issue_id in matches:
                issue_commit_map.setdefault(issue_id, []).append(commit)
        print(f"Issues with commits mapped: {len(issue_commit_map)}")
        return issue_commit_map

class FeatureExtractor:
    def __init__(self, issues, issue_commit_map):
        self.issues = issues
        self.issue_commit_map = issue_commit_map

    def get_in_progress_duration(self, changelog):
        from_status_time = None
        to_done_time = None
        for history in changelog.get('histories', []):
            for item in history.get('items', []):
                if item['field'] == 'status':
                    if item['toString'].lower() == 'in progress':
                        from_status_time = history['created']
                    elif item['toString'].lower() in {'done', 'resolved', 'closed', 'fixed'} and from_status_time:
                        to_done_time = history['created']
                        break
            if to_done_time:
                break
        if from_status_time and to_done_time:
            start = datetime.strptime(from_status_time, '%Y-%m-%dT%H:%M:%S.%f%z')
            end = datetime.strptime(to_done_time, '%Y-%m-%dT%H:%M:%S.%f%z')
            return (end - start).total_seconds() / 3600
        return None

    def extract(self):
        print("Extracting features from issues...")
        data = []
        skipped_no_commits = 0

        for issue in self.issues:
            key = issue['key']
            fields = issue['fields']
            changelog = issue.get('changelog', {})

            commits = self.issue_commit_map.get(key, [])
            if not commits:
                skipped_no_commits += 1
                continue  # Skip issues with no commits

            created = datetime.strptime(fields['created'], '%Y-%m-%dT%H:%M:%S.%f%z')
            resolved = datetime.strptime(fields['resolutiondate'], '%Y-%m-%dT%H:%M:%S.%f%z') if fields['resolutiondate'] else None
            time_to_resolve = (resolved - created).total_seconds() / 3600 if resolved else None
            time_in_progress = self.get_in_progress_duration(changelog)

            description_length = len(fields['description']) if fields['description'] else 0
            num_comments = fields['comment']['total'] if fields['comment'] else 0

            lines_added = sum(commit.stats.total.get('insertions', 0) for commit in commits)
            lines_deleted = sum(commit.stats.total.get('deletions', 0) for commit in commits)
            lines_updated = min(lines_added, lines_deleted)

            total_files_changed = sum(len(commit.stats.files) for commit in commits)
            num_commits = len(commits)

            data.append({
                'issue_id': key,
                'issue_type': fields['issuetype']['name'],
                'priority': fields['priority']['name'],
                'description_length': description_length,
                'num_comments': num_comments,
                'time_to_resolve_hours': time_to_resolve,
                'time_in_progress_hours': time_in_progress,
                'lines_added': lines_added,
                'lines_deleted': lines_deleted,
                'lines_updated_est': lines_updated,
                'total_files_changed': total_files_changed,
                'num_commits': num_commits
            })

        print(f"Issues included in dataset: {len(data)}")
        print(f"Issues skipped (no commits): {skipped_no_commits}")
        return pd.DataFrame(data)

class EffortDatasetBuilder:
    def __init__(self, config):
        self.config = config
        self.jira_client = JiraClient(config)
        self.repo_analyzer = GitRepoAnalyzer(config)

    def build_and_save(self, output_file=None):
        print(f"\n--- Building dataset for {self.config.name} ---")

        issues = self.jira_client.fetch_issues()

        issue_commit_map = self.repo_analyzer.map_issues_to_commits(issues)

        extractor = FeatureExtractor(issues, issue_commit_map)
        dataset = extractor.extract()

        output_file = output_file or f"{self.config.jira_key.lower()}_effort_dataset.csv"
        dataset.to_csv(output_file, index=False)
        print(f"✅ Dataset saved to '{output_file}'.")

# -------------------- Run --------------------

if __name__ == "__main__":
    CALCITE_CONFIG = ProjectConfig(
        name="Calcite",
        jira_key="CALCITE",
        repo_url="https://github.com/apache/calcite.git",
        local_repo_path="./calcite"
    )

    IVY_CONFIG = ProjectConfig(
        name="Ant Ivy",
        jira_key="IVY",
        repo_url="https://github.com/apache/ant-ivy.git",
        local_repo_path="./ant-ivy"
    )

    # Choose config
    config = CALCITE_CONFIG
    # config = IVY_CONFIG
    builder = EffortDatasetBuilder(config)
    builder.build_and_save()
