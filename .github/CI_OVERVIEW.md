# Overview
This project's Continuous Integration (CI) pipeline is primarily constructed using GitHub Actions, with some assistance from TeamCity to generate a base image from a private repository. The primary configuration file, main.yml, incorporates checks for Pytest, Mypy, and code style adherence.

At present, CI checks do not impede the progress of pull requests. However, it is anticipated that such checks will become mandatory in the future.

# Quick intro into github actions concepts:
- Event Trigger: GitHub Actions starts by listening to events that occur within your GitHub repository. These events can include code pushes, pull requests, issues, or custom events triggered by your actions.

- Workflow Configuration: You define the automation process using a configuration file called a "workflow." This file, typically named .github/workflows/main.yml, resides in your repository's .github directory. It specifies when and how actions should run.

- YAML Syntax: The workflow configuration is written in YAML (Yet Another Markup Language). It's easy to read and write, making it accessible to both developers and non-developers.

- Jobs and Steps: Within a workflow, you define one or more "jobs." Each job represents a unit of work, such as building, testing, or deploying your application. Jobs can run concurrently or sequentially. 

- Job Dependencies: Workflows can define dependencies between jobs. You can specify that one job should only start after another job has successfully completed. This allows you to create complex workflows with multiple steps and dependencies.

- Steps: Jobs consist of multiple "steps." Steps are individual tasks that execute a specific action or command, like compiling code, running tests, or deploying to a server. You can use pre-built actions from the GitHub Marketplace or create custom actions for your specific needs.

- Environment and Context: You can define environments and contexts for your workflows, providing a controlled environment for your actions to execute. This allows you to set up specific variables and secrets securely.

- Event Context: GitHub Actions provides contextual information about the event that triggered the workflow, including details about the pull request, commit, or issue that initiated the workflow. This context can be used to make decisions within your workflow.

- Parallelism: GitHub Actions offers parallel execution, allowing multiple jobs to run simultaneously. This can significantly speed up your workflows, especially for tasks that don't depend on each other.

- Status and Reporting: As actions execute, GitHub Actions provides real-time status updates and logs for each step and job. You can monitor the progress and identify any issues that arise during the workflow.

- Running Job in Container: GitHub Actions allows you to specify the execution environment for your jobs using containers. You can define the container image, which includes the necessary tools, dependencies, and runtime environment for your actions.

- Using Third-Party Workflows: You can reuse and incorporate third-party workflows from the GitHub Marketplace into your own workflows. These pre-built workflows cover a wide range of tasks, such as code quality checks, notifications, and deployment to various platforms.

- Conditional Execution: GitHub Actions supports conditional execution of steps and jobs based on specific criteria. You can define conditions using expressions, event types, or environment variables. This flexibility enables you to control the flow of your workflow based on the context.

- Passing Values Between Jobs: In multi-job workflows, you can pass data and values between jobs using artifacts or workflow-level variables. This enables you to share results, artifacts, or calculated values from one job to another, facilitating complex workflows.

- Matrix Execution of Jobs: GitHub Actions allows you to define matrix workflows, where a single job is run multiple times with different sets of parameters. This is useful for tasks like running tests across different operating systems or Python versions simultaneously, improving efficiency.

- Artifacts and Outputs: Jobs in GitHub Actions can produce artifacts, which are files or directories generated during the workflow execution. These artifacts can be saved and used in subsequent jobs or as outputs of the workflow.

- Parallelism in Matrix Jobs: When using matrix execution, GitHub Actions can run multiple matrix jobs in parallel, making it even more efficient for testing across various configurations.

# General notes 
- We employ a private container registry [https://console.cloud.yandex.ru/folders/b1gb03nv9vogau0an4fa/container-registry/registries/crpsufj53lhl0via9iam/overview] and utilize self-hosted runners [https://github.com/datalens-tech/shr_ansible].

- The majority of our jobs are executed within Docker containers, a practice adopted to minimize interactions with the host system and reduce potential conflicts between concurrent runs on the same host.

- We store a variety of sensitive information in GitHub Actions (GHA) secrets. It's imperative to exercise caution when working with these secrets, even though GitHub is working towards suppressing their visibility in the output.

- GitHub runners mount the "workspace" directory into containers. As a preventive measure, we perform cleanup operations before using checkouts, and this is done intentionally to avoid any issues.

- The transmission of values between jobs is subject to GitHub's security measures. Therefore, in the "main.yml" configuration file, the "split" job emits four lists of strings instead of outputting a JSON object with a dictionary of lists. GitHub attempts to conceal what it interprets as a secret, often assuming that these are curly brackets, particularly when complex JSON objects are involved in our secrets.

# Specific job comments

## 


## debug.yml
This workflow configuration serves as a placeholder intended for replacement in private branches when the need arises to create a new workflow. GitHub permits the execution of workflows defined in non-default branches, but this capability necessitates the existence of a workflow with the exact same configuration in the default branch. While it's possible to experiment with other workflow configurations, maintaining a dedicated one specifically for this purpose is generally considered a more structured and advisable approach within the context of GitHub Actions, which are automated tasks or processes that can be executed in response to events in your GitHub repository, typically utilized by software developers for various purposes such as testing, building, and deploying software projects.

## build_debian_docker.yml

This workflow represents the only process that is executed directly on the runner host. It involves the creation of a Debian Docker image, complete with an environment that includes the necessary tools for Docker building and running. This image is intended for use whenever there is a requirement to construct a new Docker image while a GitHub Actions (GHA) job is running within a container. Importantly, this workflow does not depend on code checkout, which helps reduce interaction with the runner filesystem.

The resulting built image is subsequently uploaded to our container registry. The workflow is scheduled to automatically initiate a rebuild once a week, or it can also be triggered manually when needed. This ensures that our Docker image remains up-to-date and readily available for development and deployment tasks.

# main.yml

In the main.yml workflow, we perform code checks using several tools: pytest, mypy, and code quality assessment tools like isort, black, and ruff. To streamline our workflow and reduce unnecessary work, when the workflow is triggered, we attempt to identify which packages have been impacted by the changes in a pull request. We then run tests exclusively on these affected packages. It is also possible to manually specify a list of packages to test, thereby overriding the automatic detection.

## Triggers:

This workflow is currently initiated through two primary methods: the submission or update of a pull request (PR) and manual execution. In the latter scenario, you have the flexibility to specify a list of packages for verification and can opt to skip the pytest checks by enabling a corresponding flag.
Controlling Concurrent Execution

## Concurrency:
To prevent excessive workflow runs, we have limited it to one execution per pull request. If a new workflow run is triggered while an older one is still in progress, the older run will be automatically canceled to maintain efficiency and avoid redundancy.

## Step gh_build_image

Within this step, we construct Docker images with all the necessary packages. These images used in all subsequent steps in our workflow. Currently, we are awaiting Teamcity to prepare a base image by installing third-party Python packages from a private PyPI repository containing proprietary packages. Teamcity is configured to monitor this repository and initiate a build process whenever changes occur. The base images are uniquely tagged using a sha256 value that is computed by a script (please refer to the link here for more details on this script). Build are done using ops/ci/gha_build_bake.sh script which awaits for an image generated by Teamcity and invokes bake to build images.

We perform a cleanup operation on the workspace directory to ensure the correctness of the checkout process. This is crucial to prevent any interference from the git/GitHub/GitHub Actions runner, which could impact subsequent runs.

## Step router
Within this particular step, our primary objective is to identify the packages that have been influenced by alterations made in a pull request or within the changes between the current branch and its base. If the workflow is manually initiated, the initial set of packages can be substituted with a user-defined list. Otherwise, the altered files are ascertained from the Git repository using the /ci/gh_list_changes.sh script. Following this, we determine the set of packages to be tested by employing the detect-affected-packages command from the mainrepo/terrarium/bi_ci utility package. The resulting list of packages is then communicated to the GitHub output.

## Pytest split
Within this step, we take the list of affected packages and transform it into what we call 'pytest targets.' These targets will later be utilized by various pytest_* jobs, each contingent on its specific characteristics. This step is reliant on a router and invokes the 'split-pytest-tasks' command from the 'mainrepo/terrarium/bi_ci' package. We may encounter repeated invocations of this command due to certain limitations in GitHub Actions. It's possible that we can optimize this process by configuring multiple variables in a single operation, although I haven't yet discovered how to do so.

The 'split-pytest-tasks' command considers the list of affected packages and examines each one for the presence of a custom section called "datalens.pytest" in the 'pyproject.toml' file within each package. If this section is not found, a "base" case is assumed for the entire package.

## Pytest [base,fat,exp_public,ext_private]

Within this section, we execute pytest, utilizing a designated list of targets, each tailored to a specific type of runner. Currently, we maintain four distinct categories of targets/runners:

base: These targets are designed for runners marked with the 'light' label. They primarily handle test tasks that do not require external network access and do not consume a significant amount of RAM. While there is no fixed memory threshold at this time, if you anticipate a need for several gigabytes of RAM, it is advisable to consider a different runner category.

fat: This category is intended for runners labeled as 'fat.' These targets are suitable for test scenarios that demand substantial RAM resources, often due to the presence of numerous service containers that need to be initiated.

ext public: This designation applies to a targets requiring public internet access. 

ext private: Similarly, this category is reserved for targets requiring access to the private resources.

