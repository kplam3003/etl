>Author: hieu.hoang@tpptechnology.com
>Created at: 2023-02-16
>Updated at: 2023-02-17

This is a guide for deployment process, and the deploy steps.
Must know:
- Git
- GitLab
- SSH
- Some basic Linux command line
- Some basic understanding of Docker and Docker Compose

# Overview
We follow a trunk-based development cycle. 
- Development happens in `develop` branch, and deployed to development environment. 
- After the feature is tested by developers/engineers in dev and deemed completed, changes are deployed to Staging environment using `stag` branch.
- Staging is where further testing happens by both QA Engineer and developers/engineers. QA will decide when a work item is considered completed and can be deployed to Production. 
- When it is time, changes are merged into `prod` branch more Production deployment for client's usage.

# Planning and Estimation
When a sprint starts, a deployment cycle begins:
- Planning and estimation:
	- Tickets, stories are picked from Vitruvian's Leonardo Jira board,
	- Tickets are replicated to TPP's Leonardo Jira board and are analyzed, broken down, and estimated into smaller unit of works if necessary,
	- Releases are planned. Work can span one sprint or within multiple sprints, but should aim for at least one release per sprint.
		- In case of big features or change requests, it should be splitted into small units so that merges are smaller and more frequent.
		- Smaller and more frequent merges help with code review and rollbacks, if any.

# Development
- Branches, for example feature branch `features/{ticket or short description}` or bugfix branch `bugfix/{ticket or short description}` that will contain the new development codes are checked out from `develop`
- Commits are made to the branch above until a unit of work or a feature is completed.
- A Merge Request is created into `develop` branch.
- MR is reviewed, approved, and merged into `develop` branch.
- Deployment pipeline is triggered and new codes are deployed to Development environment.
- Developers run and verify the correctness of new code to the best of their ability.

# Staging Release
- After a planned amount of work is finished and tested in development environment, a release is made.
- Before making a release, `CHANGELOG.md` file must be updated with the items that are releases.
	- Usually, we add the tickets that contains the work descriptions.
	- For changes that are not recorded in tickets, for example extra optimization, bugfixes, scraper maintenance, just include a small description for each of the change.
- After updating the changelog and commited to `develop`, a release branch is checked-out from `develop`:
	- Branch name format: `releases/vX.Y.Z`,
	- `X`: wave number. At the point of writing, it is 5,
	- `Y`: sprint number, for example 25,
	- `Z`: release number within a sprint, for example 1 for first release in sprint,
	- For example: Wave 5, Sprint 25, release number 2: `releases/v5.25.2`
- A merge request to `stag` branch is created, with the branch name as release name, and changelog as the description.
- MR is reviewed, approved, and merged into `stag` branch,
- Deployment pipeline is trigger and new codes are deployed to Staging environment,
- Developers verify the release, and the announce the release to `Leonardo General` Teams group, and update the status of relevant tickets to `Ready for QA`.
- QA starts testing the deployed work based on criteria in the ticket.

# Testing and Bugfix
- After a release happens, especially first release of a sprint, new features can start being developed. As a result, `develop` may contains unfinished code not fit for release.
- When bugs are found in a feature right after release by QA, there are usually 2 cases:
	- Minor bugs, which tend to be relatively simple to fix. 
		- In this case, fixes can be commited directly into the release branch, and merged again into `stag`, then tested on Staging environment by both QA and developer that issues the fix.
		- If the bug has lower priority, it can also be commited to `develop` and verified in Development environment, and then is included in the next release.
	- Major bugs: this needs to be fixed ASAP since it affect the completion of the tests and features
		- If `develop` already contains unfinished work, and the bug is relatively simple to fix, the fix can be commited directly into release branch and merged back to `stag` for testing on Staging.
		- However, if the bug requires major changes, affects multiple files, and generally needs more elaborated effort to fix, it should be done in a separated branch checked out from `develop`, and merged to Development environment first for testing by developer.
			- When the major fix is fixed and the next release cycle is still far, we can cherry-pick the commits that contain the fix from `develop` and merge into the relevant release branch. Then the release branch is merged into `stag` again, and deployed to Staging for testing. This is always recommended.
			- If the next release cycle is near (1-day near), the bugfix can be included in the next release. However this is not recommended.
		- In any case where `develop` does not contain any new untested changes from the next features/fixes yet (especially when there are few engineers/developers (1-2) and small number of tickets) bugfixes can be applied to `develop` and verified, and then `develop` is merged into release branch before merging release branch to `stag` again.
- When all major or critical bugs are fixed, and feature is considered complete by QA and the team, it can then be released on Production.

# Production Release
- Production release schedule depends on the completeness of features, and coordination with Vitruvian, since we should only deploy when there is no running case study or scrape running on Production to avoid complications with databases and processed being terminated to deploy new codes.
- Usually a timeframe for Production deployment is agreed upon by both sides, which is ideally every sprint. However, it may takes more than 1 sprint between releases, due to both parties wanting to complete a major, long feature.
- To deploy to Production, 
	- a Merge Request to merge the **latest release branch**, which includes all planned features and bugfixes, to `prod` branch. Similar to Staging release, the MR title should be the name of the release branch. The description, however, needs to contain **the whole changelog from the last previous release to the current release** in case of multiple releases. Bugfixes changes related to the release features should not be included in the changelog here.
	- Merge must be **squashed** to avoid irrelevant commit messages.
	- Due to squashing, there may be conflicts. To resolve conflict:
		- Pull latest from conflicting release branch to local computer,
		- Pull latest from `prod` branch to local computer,
		- **Merge `prod` to the release branch** and resolve conflict case-by-case. Most of the time, we want to keep the changes from the release, and discard the old changes from `prod`. In VSCode, we keep **current changes** and discard **incoming changes**.
		- Commit the changes to conclude the merge.
		- Push the changes in release branch after merging to GitLab. Conflicts should now be solved.
	- After merging, codes are built using Production configuration. However, deployment to Production must be run manually.
- After successful deployment, QAs and developers go through a set of smoke tests to verify the integrity of Production environment. If no major issue found, Project Lead or BA will send an email with the release note/changelog to Vitruvian.
- After Production release, any bug found either by developers, QA, or client are logged as Bug tickets and to be solved based on priority and severity. Sometimes, a fix is required ASAP during a running development cycle. 
	- A hotfix can be made by fixing directly on the latest merged release branch, and merged back to `develop` or `stag` for testing. 
	- After issue is verified to be resolve, merge back the release branch to `prod`. 
	- The release branch that contains the hotfix will also need to be merges to `develop` and `stag` if not done already while testing.