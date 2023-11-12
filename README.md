# My Knowledge Base (KB)
My journey while learning overall stuff, just in one single place to edit and look for !!!

- For Commit Messages: _"[Module] Topic: Title"_

# KB Spark

### Env var
```
export SPARK_HOME=.../MyKB/kb_spark/spark-3.2.4-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
```

### Open shell
Cd `/kb_spark/spark-3.2.4-bin-hadoop2.7/bin` dir and enter any of the available shells:
- `./pyspark` for _python_
- `./spark-shell` for _scala_

### Spark submit

```
# Spark job for M&M count
$SPARK_HOME/bin/spark-submit Python/ch2_count_agg_mnm.py data/mnm_dataset.csv
```

### Links
- _[LearningSparkV2](https://github.com/databricks/LearningSparkV2)_


# KB Airflow
Docker execution
```sh
sudo docker-compose up -d
sudo docker-compose -f _filename_.yml up -d

sudo docker-compose restart

sudo docker-compose unpause  #pause

# cd /MyKB/kb_airflow/data_generator, and:
sudo docker cp data_dag1/. 415:/opt/airflow/data_generator/data_dag1/. #kind-of refresh
```



# KB Jupyter Notebook

Install python virtual env as kernel to run Jupyter notebooks
```sh
pipenv install jupyter
pipenv install ipykernel
pipenv run python -m ipykernel install --user --name=<virtual_env_name>
jupyter notebook  # init jupyter
```

Command for this project
```sh
pipenv run python -m ipykernel install --user --name=kb_jupyter
```

Kernel spec
```sh
jupyter kernelspec list
jupyter kernelspec uninstall pandasprom
```


## Emoji Base
| Emoji | Emoji name | Description |
| :------: | -------------- | --------------- |
| :heavy_plus_sign: | `:heavy_plus_sign:` | Add new feature, e.g., new report |
| :factory_worker: | `:factory_worker:` | _Work in progress..._ |
| :memo: | `:memo:` | Modify functionality like adding a new column |
| :wrench: | `:wrench:` | Code Refactor. Ex: abstract class for _xyz_ topic |
| :x: | `:x:` | Code removal. Ex: dropping a table no longer used |
| :broom: | `:broom:` | Cleaning-up, such as unneeded comments, vars, etc. |
| :writing_hand: | `:writing_hand:` | Update documentation |
| :bug: | `:bug:` | Bug fixing |
| :bullettrain_front: | `:bullettrain_front:` | For chore changes |
| :test_tube: | `:test_tube:` | Unit tests adjustments |
| :rocket: | `:rocket:` | CI/CD changes |
