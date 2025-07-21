## Project Structure 

The project is organized as follows:

```
.
├── data/
│   ├── data_dictionary_college_scorecard/
│   │   └── CollegeScorecardDataDictionary.xlsx
│   ├── processed_data/
│   │   ├── (SCRIPT OUTPUT: Output of the identify_and_extract_% scripts.)
│   │   └── data_dictionary/
│   │       ├── college_scorecard/
│   │       │   └── (SCRIPT OUTPUT: College Scorecard data dictionary CSVs, e.g., 2020.csv)
│   │       └── ipeds/
│   │           └── (SCRIPT OUTPUT: IPEDS data dictionary CSVs, e.g., 2020.csv)
│   │
│   └── raw_data/
│       ├── college_scorecard/
│       │   └── (USER INPUT: your College Scorecard CSVs, e.g., MERGED2022_23_PP.csv)
│       └── ipeds/
│           └── (USER INPUT: your IPEDS Access databases, e.g., IPEDS202223.accdb)
│
├── notebooks/
│   │   exploratory_data_analysis.ipynb
│   │   socioeconomic_data_dictionary.ipynb
│   │
└── scripts/
    ├── identify_and_extract_ipeds_socioeconomic_factors.py
    └── identify_and_extract_college_scorecard_socioeconomic_factors.py
```

-----

# Getting Started

This section will guide you through setting up the project locally.

## Prerequisites

List any software, libraries, or other dependencies that need to be installed on the user's machine before they can use your project.

  * **Python 3.x**
  * **pandas**
  * **SQLAlchemy**
  * **pyodbc**
  * **Microsoft Access Driver**: This is essential for the IPEDS data extraction script, as it interacts with `.mdb` and `.accdb` database files. This means the script is intended to be run on a **Windows** machine.

## Installation

1.  Clone the repo:
    ```sh
    git clone https://github.com/your_username/your_project_name.git
    ```
2.  Navigate to the project directory:
    ```sh
    cd ./isye6414_project
    ```
3.  Install the required Python packages:
    ```sh
    pip install pandas sqlalchemy pyodbc
    ```

-----

## How to Use the Socioeconomic Feature Extraction Scripts

The feature extraction scripts are located in the `./scripts` directory and are designed to be executed from there.

### 1\. Extracting Features from IPEDS Data (`identify_and_extract_ipeds_socioeconomic_factors.py`) 

This script processes IPEDS data stored in Microsoft Access databases (`.accdb` or `.mdb` files). Due to its reliance on Microsoft Access database drivers, this script is **intended to be executed on a Windows machine**.

#### **Data Input:**

Place your raw IPEDS Access databases into the `./data/raw_data/ipeds` directory. The script will loop through all `.accdb` (and `.mdb`) files found in this folder.

#### **Script Customization Options:**

The `identify_and_extract_ipeds_socioeconomic_factors.py` script offers several customization options within its code:

  * **Keywords for Feature Identification:** In the `__init__` method of the `SocioeconomicFactorExtractor` class (around **line 19**), you can adjust the list of keywords used to identify relevant fields in the IPEDS data. By default, it includes keywords like 'income', 'tuition', 'cost', 'price', etc.

    ```python
    self.keywords = keywords or [
        'income', 'tuition', 'cost', 'price', 'major', 'school type',
        'loan', 'debt', 'grant', 'financial', 'gender', 'race',
        'family', 'aid', 'residency', 'charge', 'expense', 'fee'
    ]
    ```

  * **Keyword Match Threshold:** On **line 66**, you can modify the `match_count` threshold. This number determines how many distinct root-keyword matches need to occur in the `longDescription` field of a variable in the data dictionary for it to be considered relevant.

    ```python
    df = df[df['match_count'] >= 2] # Default is 2
    ```

  * **Truncate Features per Keyword:** If you wish to limit the number of features extracted per keyword, you can uncomment and adjust the code at **lines 92 and 93**. By default, the script picks up to 10 variables per keyword, but this is currently not strictly enforced by the commented lines. To enforce a limit (e.g., 5 features per keyword), uncomment these lines and set `len(unique_list) == 5`.

    ```python
    # if len(unique_list) == 5: # Uncomment and adjust this line
    #    break                # Uncomment this line
    ```

#### **Execution:**

To run the script, navigate to the `scripts` directory in your terminal and execute the Python script:

```bash
cd scripts
python identify_and_extract_ipeds_socioeconomic_factors.py
```

#### **Output:**

Upon successful execution, the script will generate the following outputs:

  * **Merged Socioeconomic Features:** A single merged CSV file containing all extracted socioeconomic features from all processed IPEDS databases will be saved to:
    `./data/processed_data/extracted_ipeds_socioeconomic_features.csv`
    This file will contain a `UNITID` column (unique institution identifier) and a `year` column, in addition to the extracted socioeconomic features.

  * **Data Dictionaries:** For each processed IPEDS database, a specific data dictionary CSV file will be created, detailing the fields that were extracted. **These are crucial for understanding the meaning of the extracted features and will be very helpful for us in determining which features to include in our final analysis**. They will be saved to:
    `/data/processed_data/data_dictionary/ipeds/{db_year}.csv`
    (e.g., `/data/processed_data/data_dictionary/ipeds/2020.csv`)

-----

### 2\. Extracting Features from College Scorecard Data (`identify_and_extract_college_scorecard_features.py`) 

This script is designed to extract socioeconomic features from College Scorecard data, which is provided as **high-dimensional CSV tables**. Unlike IPEDS, where each database contains multiple distinct tables, each College Scorecard CSV file is a single, broad table. This characteristic means that each keyword you specify is likely to match a much larger number of fields compared to IPEDS.

#### **Prerequisites:**

Ensure you have the main College Scorecard Data Dictionary in the expected location for the script to load variable titles:
`./data/data_dictionary_college_scorecard/CollegeScorecardDataDictionary.xlsx`

#### **Data Input:**

Place your raw College Scorecard CSV files into the `./data/raw_data/college_scorecard` directory. The script will iterate through all `.csv` files found in this folder. Each CSV file is expected to represent data for a specific year (e.g., `MERGED2022_23_PP.csv`).

#### **Script Customization Options:**

The `CollegeScorecardFactorExtractor` class in `identify_and_extract_college_scorecard_features.py` offers specific customization points:

  * **Keywords for Feature Identification:** In the `__init__` method of the `CollegeScorecardFactorExtractor` class (around **line 22**), you can modify the `self.keywords` list. These keywords are used to identify relevant features within the `varTitle` field of the College Scorecard Data Dictionary. The default keywords provided are a concise example.

    ```python
    self.keywords = keywords or [
        'debt', 'state', 'family'
    ]
    ```

  * **Keyword Match Threshold:** On **line 44**, the script filters variables based on how many distinct root-keyword matches occur in their `varTitle`. The current setting is `df['match_count'] > 0`, meaning any field with at least one keyword match in its title will be considered.

    ```python
    df = df[df['match_count'] > 0] # Currently set to require at least one match
    ```

  * **Truncate Features per Keyword:** Similar to the IPEDS script, you can **uncomment and adjust the code at lines 74 and 75** within the `extract` method to limit the number of features extracted per keyword. This is particularly useful for College Scorecard data due to its high dimensionality and numerous matches per keyword.

    ```python
    # if len(unique_list) == 5: # Uncomment and set your desired limit
    #    break                # Uncomment this line
    ```

#### **Dynamic Fieldname Updates:**

A key aspect of this script is its ability to **dynamically update the fieldnames** it extracts. This means that for the same keyword, the actual fieldnames (column headers) extracted can change from one College Scorecard CSV table (year) to another, as the script adapts to the specific columns present in each year's dataset.

#### **Execution:**

To run the script, navigate to the `scripts` directory in your terminal and execute the Python script:

```bash
cd scripts
python extract_college_scorecard_features.py
```

#### **Output:**

Upon completion, the script will generate the following outputs:

  * **Concatenated Socioeconomic Features:** All extracted socioeconomic features from the College Scorecard CSVs will be vertically combined into a single CSV file, saved to:
    `./data/processed_data/extracted_college_scorecard_socioeconomic_features.csv`
    This combined file will include a `UNITID` column and a `year` column (derived from the filename, e.g., `MERGED2020_21_PP.csv` becomes `2020`) for each record.

  * **Data Dictionaries:** For each processed College Scorecard CSV file, a dedicated data dictionary CSV will be created. These dictionaries detail the specific fields that were extracted for that particular year's data, which is important for interpreting the output. They will be saved to:
    `/data/processed_data/data_dictionary/college_scorecard/{db_year}.csv`
    (e.g., `/data/processed_data/data_dictionary/college_scorecard/2020.csv`)

