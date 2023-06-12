# Mindex Project

This repository contains code for the Mindex challenge, which involves processing, loading data into a PostgreSQL database and creating Views from the loaded data using Python and Postgresql.

## Table of Contents
- [Project Description](#project-description)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)


## Project Description

The Mindex challenge is a data processing project that involves merging multiple CSV files, renaming columns, replacing values, and loading the data into a PostgreSQL database. The project is implemented using Python and relies on the Boto3 library for AWS S3 integration and the psycopg2 library for PostgreSQL database connectivity.

The main components of the project includes:
- `Mindexcompletecode.ipynb`: Python script that performs the data processing and database loading tasks.
- `Mindex.sql`: SQL script containing the query for the view.

## Getting Started

### Prerequisites

To run the code in this project, you need the following prerequisites:

- Python 3.x
- Boto3 library (for AWS S3 integration)
- Psycopg2 library (for PostgreSQL database connectivity)
- Pandas library 
- PostgreSQL database (you can use an existing installation or set up a new one)
- Dbeaver (to view and ensure all required data is present)

### Installation

1. Clone or download the repository from [https://github.com/sj9252/mindex/releases/tag/Mindex](https://github.com/sj9252/mindex/releases/tag/Mindex).

2. Install the required Python libraries. Run the following command in your terminal or you can also run the first cell in jupyter notebook which  has pip installation for all libraries:

   ```bash
   pip install -r requirements.txt
   
3. Set up the PostgreSQL database. You can use the provided Mindex.sql script to create the final output as required. Execute the script in your PostgreSQL database management tool (DBeaver).
   
   psql -U shakti_jagadish -d postgres -f Mindex.sql
   
## Usage

To use the Mindex project:

1. Open the Mindexcompletecode.ipynb script in a text editor or Python IDE.

2. Run the Mindexcompletecode.ipynb script:

3. The script will connect to the AWS S3 bucket, download the CSV files, perform data processing tasks, merge the datafiles, check if there are columns in the table, create new columns in database,checks for the added columns,load the data into the PostgreSQL database,checks for the loaded data.

4. Run the Mindex.sql file

5. The script will show the total yards for each receiver output Boyd Yards, Higgins Yards, Chase Yards and Win/Loss

## Contributing

Contributions to this project are welcome. If you would like to contribute, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes.
4. Commit and push your changes to your forked repository.
5. Open a pull request to the original repository.





