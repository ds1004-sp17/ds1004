# DS-GA 1004
 
The project is about processing the data of [NYC complaints](https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5).

Attention:
There are two datasets contain 2010-present dataset, one contains 52 columns and another contains 53 columns.
The one we use contains 52 columns.

## How to run

After cloning the repository,

```bash
cd src/
```  

There are a bunch of _.py_ files and several _.sh_ scripts. If you want to automatically run all the bash scripts, run the following command:

```bash
./run_all.sh [/hadoop/path/your/filename]
```

If you want to only run specific MapReduce tasks, named _foo.py_, run the following command

```bash
./run.sh foo [/hadoop/path/your/filename] # NOTE: discard the extension ".py"
```

Some tasks use SparkSQL, so run the following command

```bash
./run_sql.sh foo # without file extension ".py"
```

After processing the data by _spark_, you can run the following command to plot the data:

```bash
./run_fig process_xxx.py
```

For the scripts in the data_issue folder:
You should first use the following command:
```bash
cd data_issue
```
There are 3 shells in the folder, which can help you to run the script and get the result.
Use the following command to run:
```bash
./run_basic.sh your-input-dataset-path
./run_mv.sh your-input-dataset-path
./run_valid your-input-dataset-path
```
For example:
```bash
./run_basic.sh 311.csv
```
If the terminal could not find the input dataset, try to use a full path, like:
```bash
./run_basic.sh /user/jh5442/311.csv
```
The out put will be in the basic_out, mv_out and validation folder in the data_issue directory.
Please allow time for the scripts to run on big dataset, and try again if the spark is down during the process.

# Update basic information, counting missing values and validation
1. The PDFs will be used for the summury or report.
2. The files in data_info folder: 
   [basic_statistics_info.py] gives a general glance of the contents in each column and we can use the information to find missing values and the pattern of the columns.
   [missing_values.py] finds the missing values in the column, including blank(''), NA and so on.
   Three scripts contain the indices in file name give key, base type, semantic type and validation of NULL, VALID or INVALID   
