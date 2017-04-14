# DS-GA 1004
 
The project is about processing the data of [NYC complaints](https://data.cityofnewyork.us/Social-Services/311/wpe2-h2i5).

## How to run

After cloning the repository,

```bash
cd src
```  

There are a bunch of _.py_ files and several _.sh_ scripts. If you want to automatically run all the bash scripts, run the following command:

```bash
./run_all.sh
```

If you want to only run specific MapReduce tasks, named _foo.py_, run the following command

```bash
./run.sh foo # NOTE: discard the extension
```

After processing the data by _spark_, you can run the following command to plot the data:

```bash
./run_fig
```
# Update basic information, counting missing values and validation
1. The PDFs will be used for the summury or report.
2. The files in data_info folder: 
   [basic_statistics_info.py] gives a general glance of the contents in each column and we can use the information to find missing values and the pattern of the columns.
   [missing_values.py] finds the missing values in the column, including blank(''), NA and so on.
   Three scripts in validation folder give key, base type, semantic type and validation of NULL, VALID or INVALID   
3. To run: spark-submit python-file-name input-dataset

# Problems to be solved
1. There are 51 output folders, so the "hfs -getmerge" need to be used many times
