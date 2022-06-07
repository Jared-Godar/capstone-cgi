# CGI Onshore Data Engineering Capstone

> Jared A. Godar

## About this project

[Trello Board](https://trello.com/invite/b/D1zOnnNU/83cf7c878e0aa6b525f4635f5af2bc47/capstone)

This will serve as the foundation of a ten to fifteen minute pre-recorded presentation highlighting what I have learned in the last six weeks.

### Project goals

The goal of this project is to create a serverless end to end pipeline to use Databricks and Selenium to scrape upcoming concert information from venues near me, store the findings on amazon cloud, and output a dashboard of upcoming concerts.

### Project Description

This project provides an opportunity to feature many of the techniques we have learned. I will be using python, sql, Databricks, and AWS cloud.

## Data Dictionary

- This is what the scraped tables being imported will look like

| Feature | Datatype              | Description         |
| :------ | :-------------------- | :------------------ |
| venue   | object                | Venue Name          |
| date    | datetime (mm/DD/YYYY) | Concert Date        |
| time    | datetime HH:MM        | Start Time          |
| price   | float                 | Concert Price (USD) |

---

## The Plan

1. Acquire and clean the data
   1. For the MVP, I will manually create a `.csv` file with some concert information in order to build the pipeline.
   2. Once the pipeline is up and running, I will use Selenium and/or Beautiful Soup to scrape this data from venue websites in an automated way
2. Build Amazon pipeline
   1. Use Amazon CLI to place `.csv` file into a S3 bucket
   2. Lambda function takes the S3 file to Redshift and posts to SQS2
   3. Redshift triggers another lambda to unload directory from redshift
   4. Third lambda adds to dynamo db
   5. Data to redshift
   6. Message sqs - lambda subscribed
   7. Lambda fires taking data to dynamodb
   8. Boto3 source to talk to redshift
3. Output dashboard
   1. Website where concert information is displayed

---

## Tools used in this project

- [Poetry](https://towardsdatascience.com/how-to-effortlessly-publish-your-python-package-to-pypi-using-poetry-44b305362f9f): Dependency management - [article](https://towardsdatascience.com/how-to-effortlessly-publish-your-python-package-to-pypi-using-poetry-44b305362f9f)
- [hydra](https://hydra.cc/): Manage configuration files - [article](https://towardsdatascience.com/introduction-to-hydra-cc-a-powerful-framework-to-configure-your-data-science-projects-ed65713a53c6)
- [pre-commit plugins](https://pre-commit.com/): Automate code reviewing formatting  - [article](https://towardsdatascience.com/4-pre-commit-plugins-to-automate-code-reviewing-and-formatting-in-python-c80c6d2e9f5?sk=2388804fb174d667ee5b680be22b8b1f)
- [DVC](https://dvc.org/): Data version control - [article](https://towardsdatascience.com/introduction-to-dvc-data-version-control-tool-for-machine-learning-projects-7cb49c229fe0)
- [pdoc](https://github.com/pdoc3/pdoc): Automatically create an API documentation for your project

## Project structure

```bash
.
├── config                      
│   ├── main.yaml                   # Main configuration file
│   ├── model                       # Configurations for training model
│   │   ├── model1.yaml             # First variation of parameters to train model
│   │   └── model2.yaml             # Second variation of parameters to train model
│   └── process                     # Configurations for processing data
│       ├── process1.yaml           # First variation of parameters to process data
│       └── process2.yaml           # Second variation of parameters to process data
├── data            
│   ├── final                       # data after training the model
│   ├── processed                   # data after processing
│   ├── raw                         # raw data
│   └── raw.dvc                     # DVC file of data/raw
├── docs                            # documentation for your project
├── dvc.yaml                        # DVC pipeline
├── .flake8                         # configuration for flake8 - a Python formatter tool
├── .gitignore                      # ignore files that cannot commit to Git
├── Makefile                        # store useful commands to set up the environment
├── models                          # store models
├── notebooks                       # store notebooks
├── .pre-commit-config.yaml         # configurations for pre-commit
├── pyproject.toml                  # dependencies for poetry
├── README.md                       # describe your project
├── src                             # store source code
│   ├── __init__.py                 # make src a Python module 
│   ├── process.py                  # process data before training model
│   └── train_model.py              # train model
└── tests                           # store tests
    ├── __init__.py                 # make tests a Python module 
    ├── test_process.py             # test functions for process.py
    └── test_train_model.py         # test functions for train_model.py
```

---

## Steps to reproduce

- [x] Read this README.md
- [ ] Install [Poetry](https://python-poetry.org/docs/#installation)
- [ ] Set up the environment:

```bash
make activate
make setup
```

## Install new packages

To install new PyPI packages, run:

```bash
poetry add <package-name>
```

## Run the entire pipeline

To run the entire pipeline, type:

```bash
dvc repo
```

## Version your data

Read [this article](https://towardsdatascience.com/introduction-to-dvc-data-version-control-tool-for-machine-learning-projects-7cb49c229fe0) on how to use DVC to version your data.

Basically, you start with setting up a remote storage. The remote storage is where your data is stored. You can store your data on DagsHub, Google Drive, Amazon S3, Azure Blob Storage, Google Cloud Storage, Aliyun OSS, SSH, HDFS, and HTTP.

```bash
dvc remote add -d remote <REMOTE-URL>
```

Commit the config file:

```bash
git commit .dvc/config -m "Configure remote storage"
```

Push the data to remote storage:

```bash
dvc push 
```

Add and push all changes to Git:

```bash
git add .
git commit -m 'commit-message'
git push origin <branch>
```

# Auto-generate API documentation

To auto-generate API document for your project, run:

```bash
make docs
```
