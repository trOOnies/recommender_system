import os

AIRBYTE_CONNECTION_IDS = {
    "movies_raw": os.environ["AB_MOVIES_RAW_CID"],
    "users_raw": os.environ["AB_USERS_RAW_CID"],
    "scores_raw": os.environ["AB_SCORES_RAW_CID"]
}
DBT_PROJECT_DIR = "recommender_system/dbt_fd/dbt_rs"
LABEL_COL = "rating"
USER_COL = "user_id"
MOVIE_COL = "movie_id"
MODELS_TUNING_FD = "files/models/tuning"
MODELS_TRAINING_FD = "files/models/training"
OBJ_METRIC = {
    "name": "rmse",
    "lower_is_better": True
}
