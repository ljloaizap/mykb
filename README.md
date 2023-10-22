# My Knowledge Base (KB)
My journey while learning overall stuff, just in one single place to edit and look for !!!

- For Commit Messages: [<Module>] <Topic>: Title


# KB Airflow
Docker execution
```sh
sudo docker-compose up -d
sudo docker-compose -f _filename_.yml up -d

sudo docker-compose restart

sudo docker-compose unpause  #pause
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
