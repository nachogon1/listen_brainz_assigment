Link of the assigment
https://c.smartrecruiters.com/sr-company-attachments-prod-aws-dc5/606dc56fe6557d6d9977ee5a/a6ed24bf-37dd-402a-82d6-fe54aac3850c?r=s3-eu-central-1

## Requirements
Python-12
MacOs (Was not tested in other OS)
"poetry-core>=2.0.0,<3.0.0"

## Installation
Install poetry
```shell
pip install poetry
```

Download the project and run the following command in the root directory of the project
```shell
poetry install
```

## Usage
From the root directory of the project run the following commands

1. Create the dataset
```shell
poetry run create-db
```

2. Ingest the data
```shell
poetry run ingest-db <Absolute_path_for_your_data>
```

3. Perform the analytic questions over the dataset
```shell
poetry run get-results
```

For the assigment specific case we placed the `dataset.txt` in `./listen_brainz_assigment/database directory`. The dataset was created in the root directory `.` .
We have not added this to the repository because of the size of the file. But it the poetry tasks should run following the instructions.


## Results
Task 1. It took around 38 minutes to ingest all the data from the dataset.txt.  Data was read and write in batches of 100k. 
We tested several methods, insert into memory and export the data to the database. But it did not add great improvements.
Batches was the fastest way to ingest the data.
Concurrency was discarded. Duckdb is not designed to do multi-process writes. We got a lock error and did not continue with this analysis. 

Database Schema
```
                           +----------------------+
                           |      artists         |
                           |----------------------|
                           | artist_msid  (PK)    |
                           | artist_name          |
                           | created_at           |
                           +----------------------+
                                    │
                                    │ (FK in tracks)
                                    │
                           +----------------------+
                           |      tracks          |
                           |----------------------|
                           | recording_msid (PK)  |
                           | track_name           |
                           | artist_msid  (FK) ────────────┐
                           | release_msid (FK) ───┐        │
                           | recording_mbid       │        │
                           | release_group_mbid   │        │
                           | isrc                 │        │
                           | spotify_id           │        │
                           | tracknumber          │        │
                           | track_mbid           │        │
                           | created_at           │        │
                           +----------------------+        │
                                    │                      │
                ┌───────────────────┼──────────────────────┼─────────────────────┐
                │                   │                      │                     │
+----------------------+   +----------------------+  +----------------------+  +----------------------+
|    track_tags        |   |     listens          |  |     track_works      |  |      releases        |
|----------------------|   |----------------------|  |----------------------|  |----------------------|
| recording_msid (FK)  |   | user_name            |  | recording_msid (FK)  |  | release_msid  (PK)   |
| tag           (PK)   |   | recording_msid (FK)  |  | work_mbid     (FK)   |  | release_mbid         |
| created_at           |   | listened_at          |  | created_at           |  | release_name         |
+----------------------+   | created_at           |  +----------------------+  | created_at           |
                           | (PK: listened_at,    |             |              +----------------------+
                           |  recording_msid,     |             |
                           |  user_name)          |             |
                           +----------------------+             |
                                                                |
                                                       +----------------------+
                                                       |       works          |
                                                       |----------------------|
                                                       | work_mbid   (PK)     |
                                                       | created_at           |
                                                       +----------------------+

          Note: Both **listens** and **track_works** reference the **tracks** table
                via the recording_msid field. This creates an indirect link between
                track_works and listens.

```

Task 2.

Full results can be found in `listen_brainz_assigment/results` directory

Task 2. a)

```
┌────────────────┬──────────────┐
│   user_name    │ listen_count │
│    varchar     │    int64     │
├────────────────┼──────────────┤
│ hds            │        46885 │
│ Groschi        │        14959 │
│ Silent Singer  │        13005 │
│ phdnk          │        12861 │
│ 6d6f7274686f6e │        11544 │
│ reverbel       │         8398 │
│ Cl�psHydra     │         8318 │
│ InvincibleAsia │         7804 │
│ cimualte       │         7356 │
│ inhji          │         6349 │
├────────────────┴──────────────┤
│ 10 rows             2 columns │
└───────────────────────────────┘

┌────────────┐
│ user_count │
│   int64    │
├────────────┤
│         75 │
└────────────┘

┌─────────────────┬──────────────────────────────────────┬─────────────────────────────────────────────────┬─────────────────────┐
│    user_name    │            recording_msid            │                   track_name                    │    first_listen     │
│     varchar     │                 uuid                 │                     varchar                     │      timestamp      │
├─────────────────┼──────────────────────────────────────┼─────────────────────────────────────────────────┼─────────────────────┤
│ bonedriven      │ 9139b953-dbfc-4d7d-98ce-b0c593c0ad9e │ Everything Zen (The Lhasa Fever Mix)            │ 2019-02-26 11:02:33 │
│ hds             │ 0e8b33d2-3a87-40bd-bc02-9b223353af5b │ Love Within Beauty                              │ 2019-03-02 06:36:56 │
│ Bound2Fate      │ f843d888-fa16-477a-b527-e9126d2f1f17 │ Home Invasion                                   │ 2019-01-01 00:07:01 │
│ N1CK3Y          │ 483222c4-1a96-4e94-861f-744e372e6f24 │ Shitstorm                                       │ 2019-01-01 00:00:48 │
│ SadGen          │ dadf7010-a64a-42c2-8b9e-233805faad65 │ Anathema                                        │ 2019-02-24 23:56:21 │
│ lee_cz          │ af4ddbc2-e290-43c9-9a66-0a1fc02686b8 │ Emperor's New Clothes                           │ 2019-01-01 12:02:11 │
│ m0rth0n         │ 4284a790-3042-4a27-9773-aeeb767b2623 │ THRU THE VOID                                   │ 2019-04-12 22:01:49 │
│ mankymusic      │ d3c6ee38-aa14-4b12-b5d6-5c3828e63dd1 │ Lemniscaat: Refrain                             │ 2019-01-01 02:08:44 │
│ meisterhauns    │ d87640d9-8044-44cc-bacb-cce2d5183480 │ S.A.D.O.S. (feat. Jaq)                          │ 2019-02-26 22:12:31 │
│ miketwiss       │ 8011f7e5-9eda-4abf-bb9b-776c7e63a634 │ What Happened (mastered)                        │ 2019-02-24 21:26:31 │
│     ·           │                  ·                   │   ·                                             │          ·          │
│     ·           │                  ·                   │   ·                                             │          ·          │
│     ·           │                  ·                   │   ·                                             │          ·          │
│ arcadian99      │ 9849cba8-07c4-41d0-be74-223f8bdacb42 │ Crying                                          │ 2019-02-21 12:23:16 │
│ cirfis          │ c16a9d5c-0cf2-470d-86fe-796f2f4f107f │ Crossing the Frame                              │ 2019-01-13 02:23:07 │
│ kingjan1999     │ 01539bec-5391-4afe-b881-d6942bf66b95 │ Bratislava                                      │ 2019-03-24 15:22:53 │
│ kmontenegro     │ 4c69359e-68d0-4b1f-97ee-e6c9258abdea │ Work                                            │ 2019-01-01 00:12:23 │
│ linkmath        │ e8dba4af-d142-42fc-8398-4db6431f1cca │ Taki Taki (part. Selena Gomez, Ozuna e Cardi B) │ 2019-01-16 22:28:10 │
│ maeve1916       │ 608e9ac8-79f0-4926-b15f-cb4960cd1936 │ Freedom                                         │ 2019-01-01 19:30:38 │
│ meashishsaini   │ 25636f85-9108-4594-aeb0-dc5575e7dafe │ Bambi                                           │ 2019-01-04 12:05:16 │
│ satanshunger    │ 01ea1e12-48d6-41f0-8c3b-a87aa8000971 │ Cartão de Visita                                │ 2019-01-01 16:38:56 │
│ zebedeemcdougal │ bbee1482-926d-4f98-a554-e0b558c9014a │ Roygbiv                                         │ 2019-01-02 12:37:20 │
│ †AtzzkiySotona† │ 937b6495-4d51-415e-a63f-cdd04307cb49 │ Meridian                                        │ 2019-01-05 11:13:45 │
├─────────────────┴──────────────────────────────────────┴─────────────────────────────────────────────────┴─────────────────────┤
│ 202 rows (20 shown)                                                                                                  4 columns │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
Task 2. b)

```

┌─────────────────┬───────────────────┬────────────┐
│      user       │ number_of_listens │    date    │
│     varchar     │       int64       │    date    │
├─────────────────┼───────────────────┼────────────┤
│ 6d6f7274686f6e  │               204 │ 2019-01-27 │
│ 6d6f7274686f6e  │               198 │ 2019-01-14 │
│ 6d6f7274686f6e  │               196 │ 2019-03-15 │
│ Adsky_traktor   │               109 │ 2019-01-03 │
│ Adsky_traktor   │                99 │ 2019-01-05 │
│ Adsky_traktor   │                86 │ 2019-01-04 │
│ AllSparks       │               114 │ 2019-01-31 │
│ AllSparks       │                81 │ 2019-01-23 │
│ AllSparks       │                72 │ 2019-01-11 │
│ AlwinHummels    │                 1 │ 2019-02-24 │
│    ·            │                 · │     ·      │
│    ·            │                 · │     ·      │
│    ·            │                 · │     ·      │
│ yellams         │               160 │ 2019-02-08 │
│ zebedeemcdougal │                82 │ 2019-02-22 │
│ zebedeemcdougal │                81 │ 2019-02-02 │
│ zebedeemcdougal │                80 │ 2019-01-11 │
│ zergut          │               120 │ 2019-01-02 │
│ zergut          │                59 │ 2019-02-26 │
│ zergut          │                42 │ 2019-03-05 │
│ †AtzzkiySotona† │                39 │ 2019-01-09 │
│ †AtzzkiySotona† │                37 │ 2019-03-21 │
│ †AtzzkiySotona† │                35 │ 2019-03-19 │
├─────────────────┴───────────────────┴────────────┤
│ 577 rows (20 shown)                    3 columns │
└──────────────────────────────────────────────────┘
```

Task 2. c)
```
┌─────────────────────┬─────────────────────┬─────────────────────────┐
│        date         │ number_active_users │ percentage_active_users │
│      timestamp      │        int64        │         double          │
├─────────────────────┼─────────────────────┼─────────────────────────┤
│ 2019-01-01 00:00:00 │                  72 │       35.64356435643565 │
│ 2019-01-02 00:00:00 │                  95 │       47.02970297029703 │
│ 2019-01-03 00:00:00 │                 103 │       50.99009900990099 │
│ 2019-01-04 00:00:00 │                 107 │       52.97029702970297 │
│ 2019-01-05 00:00:00 │                 108 │       53.46534653465346 │
│ 2019-01-06 00:00:00 │                 110 │       54.45544554455446 │
│ 2019-01-07 00:00:00 │                 114 │       56.43564356435643 │
│ 2019-01-08 00:00:00 │                 113 │       55.94059405940594 │
│ 2019-01-09 00:00:00 │                 115 │       56.93069306930693 │
│ 2019-01-10 00:00:00 │                 115 │       56.93069306930693 │
│          ·          │                   · │               ·         │
│          ·          │                   · │               ·         │
│          ·          │                   · │               ·         │
│ 2019-04-06 00:00:00 │                  97 │       48.01980198019802 │
│ 2019-04-07 00:00:00 │                  95 │       47.02970297029703 │
│ 2019-04-08 00:00:00 │                  93 │      46.039603960396036 │
│ 2019-04-09 00:00:00 │                  95 │       47.02970297029703 │
│ 2019-04-10 00:00:00 │                  93 │      46.039603960396036 │
│ 2019-04-11 00:00:00 │                  92 │       45.54455445544554 │
│ 2019-04-12 00:00:00 │                  94 │       46.53465346534654 │
│ 2019-04-13 00:00:00 │                  98 │       48.51485148514851 │
│ 2019-04-14 00:00:00 │                  99 │       49.00990099009901 │
│ 2019-04-15 00:00:00 │                  96 │      47.524752475247524 │
├─────────────────────┴─────────────────────┴─────────────────────────┤
│ 105 rows (20 shown)                                       3 columns │
└─────────────────────────────────────────────────────────────────────┘
```

## Challenges
Ingesting the data and modelling the database were the most challenging tasks. We tackled the problem by reading the dataset and duckdb documentation.

Still the data ingestion could be subject to improvements.



## Improvements
We could have faster ingestion times with multi batch, multi-process ingestion in a normal database.
The batch size could be further optimized.
Better validation of the data could be done.
Better handling of the data in case of duplicates.
