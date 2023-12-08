# Recommender System - Proyecto ITBA MLOps

- **Autor**: Scasso, Facundo Martín
- **Curso**: MLOps (ITBA)
- **Profesores**: Carlos Selmo y Julián Ganzabal

## Finalidad del código

Este proyecto fue realizado para el curso del ITBA de MLOps de la Escuela de Innovación, orientado a Data Scientists con experiencia previa en DS. El curso tuvo una duración de 2 meses aprox. y fue dado a fines de 2023.

## Implementaciones objetivo
- [**Airbyte**](https://airbyte.com/): EL _(Extract & Load)_ del ELT del proyecto. Fuente del proyecto: GitHub Raw.
- [**PostgreSQL**](https://www.postgresql.org/): Base de datos.
- [**DBT**](https://www.getdbt.com/): T _(transform)_ del proyecto.
- [**MLflow**](https://mlflow.org/): Registro de modelos corridos.
- [**Dagster**](https://dagster.io/): Pipeline para administrar la corrida de forma centralizada del resto de módulos e implementaciones.

## Estructura del proyecto
- `recommender_system/assets`: Assets de Dagster.
    - `raw.py`: EL de los 3 CSV fuente de información, `users`, `movies` y `scores`.
    - `transformed.py`: T de estos `DataFrames`.
    - `staging.py`: Su unión y últimas transformaciones sobre el `DataFrame` unido.
    - `splitting.py`: Generación de splits de train, validation y test.
    - `tuning.py`: Tuneo de hiperparámetros.
    - `training.py`: Entrenamiento per se del modelo.
