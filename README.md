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
