import os
import subprocess
from pathlib import Path
import pandas as pd
from shutil import rmtree
from datetime import datetime, timedelta
from tqdm import tqdm


class AuthenticationError(Exception):
    pass


def download_from_gcs(
        source_path: str,
        destination_path: str = 'data/',
        json_config_path: str = '',
        use_boto: bool = True,
        num_threads: int = 1,
        num_processes: int = 4
) -> list:
    """
    Download objects from GCS bucket to a destination path. Downloaded file will mirror the file/folder structure\
    of its origin on GCS.

    :param source_path: str: source to object. Support wildcards * and **.
    :param destination_path: str: master folder to save downloaded objects
    :param json_config_path: str: json config file for service account. Leave blank if .boto config has been \
    exported to env
    :param use_boto: bool: True if .boto config has been exported to current env. If False must give path to \
    json config.
    :param num_threads: int: number of parallel threads for downloading. Leave blank (default=1) if on Windows.
    :param num_processes: int: number of parallel processes for downloading.
    :return: a list of paths to downloaded objects
    """
    # create same folder structure as source path
    if destination_path[-1] != '/':
        destination_path += '/'
    folder_path = destination_path + '/'.join(source_path.split('/')[2:-1]) + '/'
    Path(folder_path).mkdir(parents=True, exist_ok=True)

    # gsutil command
    if use_boto:
        cmd = [
            "gsutil",
            "-q",
            "-m",
            "cp", source_path, folder_path
        ]
    else:
        if json_config_path == '':
            raise ValueError('No json_config_path provided in place of .boto config')
        cmd = [
            "gsutil",
            "-q",
            "-m",
            "-o", f"Credentials:gs_service_key_file={json_config_path}",
            "-o", f"GSUtil:parallel_process_count={num_processes}",
            "-o", f"GSUtil:parallel_thread_count={num_threads}",
            "cp", source_path, folder_path
        ]
    proc = subprocess.Popen(cmd, 
                            stdout=subprocess.DEVNULL, 
                            stderr=subprocess.STDOUT) # pipe to NULL
    proc.communicate()
    return [folder_path + file for file in os.listdir(folder_path)]


def get_objects_info(
        bucket_name: str,
        day: datetime = None,
        step: str = 'all',
        json_config_path: str = '',
        use_boto: bool = True,
        num_threads: int = 1,
        num_processes: int = 4
) -> pd.DataFrame:
    """
    Construct a pd.DataFrame of objects within given bucket_name, step (optional), and day (optional).

    :param bucket_name: str: name of bucket to read from
    :param day: datetime/date: day to read from. If None then read everything.
    :param step: str: step (sub-folder) to read from. If 'all' reads everything.
    :param json_config_path: str: json config file for service account. Leave blank if .boto config has been \
    exported to env
    :param use_boto: bool: If True use .boto config file in env (assumed exported). If False must give path to \
    json config.
    :param num_threads: int: number of parallel threads for downloading. Leave blank (default=1) if on Windows.
    :param num_processes: int: number of parallel processes for downloading.
    :return: pd.DataFrame: pd.DataFrame contain info of all objects matched.
    :raises:
        ValueError: if use_boto is False and json_config_path is not provided
        AuthenticationError: if authentication to GCS failed
        FileNotFoundError: if no object matched
    """
    columns = ('file_size', 'time_created', 'file_path', 'bucket', 'folder', 'batch',
               'filename', 'batch_path', 'company', 'source')
    # parse day into appropriate string. If None get all days. NOT RECOMMENDED.
    if day is None:
        match_part = '**'
    else:
        day_string = datetime.strftime(day, '%Y%m%d')
        match_part = f'*{day_string}*'
    # point to appropriate sub-folder/step if specified
    if step == 'all':
        gcs_path = f"gs://{bucket_name}/*/{match_part}/*"
    else:
        gcs_path = f"gs://{bucket_name}/*/{match_part}/*"

    # authentication method check.
    if use_boto:
        ls_cmd = [
            "gsutil", "-m", "ls", "-l",
            gcs_path
        ]
    else:
        if json_config_path == '':
            raise ValueError('No json_config_path provided in place of .boto config')
        ls_cmd = [
            "gsutil", "-m",
            "-o", f"Credentials:gs_service_key_file={json_config_path}",
            "-o", f"GSUtil:parallel_process_count={num_processes}",
            "-o", f"GSUtil:parallel_thread_count={num_threads}",
            "ls", "-l", gcs_path
        ]

    # get matched file info
    ls_output, _ = subprocess.Popen(
        ls_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    ).communicate()
    split_ls_output = ls_output.decode('UTF-8').split('\n')

    # catch common exceptions
    if 'CommandException' in split_ls_output[0]:
        # match Exception when running codes. Raise file not found error
        raise FileNotFoundError(f'{gcs_path} does not match any object')
    if 'ServiceException: 401' in split_ls_output[0]:
        raise AuthenticationError('Check config json or boto path')

    # parse ls output into dataframe
    info_df = pd.DataFrame(
        [s.lstrip().split() for s in split_ls_output[:-2]],  # last 2 lines are number of files, and an empty line as
        columns=['file_size', 'time_created', 'file_path']
    )
    info_df['time_created'] = info_df.time_created.apply(pd.to_datetime)
    info_df['file_size'] = info_df.file_size.astype(int)
    info_df = info_df.dropna(subset=['time_created'])
    info_df = pd.concat(  # create additional fields for info_df
        [
            info_df,  # original dataframe
            pd.DataFrame(  # split source path into appropriate parts
                info_df.file_path.apply(lambda s: s.split('/')[2:6]).tolist(),
                columns=['bucket', 'folder', 'batch', 'file_name']
            ),
            pd.DataFrame(  # construct the batch paths
                info_df.file_path.apply(lambda s: 'gs://' + '/'.join(s.split('/')[2:5]) + '/*').tolist(),
                columns=['batch_path']
            ),
            pd.DataFrame(  # extract company name and source name from filename
                info_df.file_path.apply(lambda s: s.split('/')[4].split('-')[0:2]).tolist(),
                columns=['company', 'source']
            )
        ],
        axis=1
    )
    return info_df


def download_gcs_to_dataframe(
        source_path: str,
        destination_path: str = 'data/',
        json_config_path: str = '',
        use_boto: bool = True,
        num_threads: int = 1,
        num_processes: int = 4,
        delete: int = False,
        print_empty: bool = False,
        progress_bar: bool = True
) -> pd.DataFrame:
    """
    Download objects from GCS bucket to a destination path and return a pandas DataFrame of the downloaded objects. \
    Call download_from_bucket to download.

    :param source_path: str: source to object. Support wildcards * and **.
    :param destination_path: str: master folder to save downloaded objects
    :param json_config_path: str: json config file for service account. Leave blank if .boto config has been \
    exported to env
    :param use_boto: bool: If True use .boto config file in env (assumed exported). If False must give path to \
    json config.
    :param num_threads: int: number of parallel threads for downloading. Leave blank (default=1) if on Windows.
    :param num_processes: int: number of parallel processes for downloading.
    :param delete: bool: if True delete downloaded files after dataframe construction
    :param print_empty: bool: if True print empty files
    :return: pd.DataFrame: a pd.DataFrame of all the files matched source_path and its info.
    :raises: ValueError: if use_boto is False and json_config_path is not provided
    """
    downloaded_files_path = download_from_gcs(source_path=source_path, destination_path=destination_path,
                                              json_config_path=json_config_path, use_boto=use_boto,
                                              num_threads=num_threads, num_processes=num_processes)

    final_df = []
    i = 0
    if progress_bar:
        it = tqdm(downloaded_files_path)
    else:
        it = downloaded_files_path
    for file in it:
        try:
            _df = pd.read_csv(file, sep='\t')
            _bucket, _folder, _batch, _name = file.split('/')[-4:]
            _df[['bucket', 'folder', 'batch', 'filename']] = _bucket, _folder, _batch, _name
        except pd.errors.EmptyDataError:
            if print_empty:
                print(f'File {file} empty')
            continue
        final_df.append(_df)
        i += 1
    if i == 0:
        return pd.DataFrame()
    if delete:
        for root, dirs, files in os.walk(destination_path):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                rmtree(os.path.join(root, d))
    return pd.concat(final_df).reset_index(drop=True)


if __name__ == '__main__':
    """
    Testing/demonstration
    """
    # some constant
    config_path = './config'
    # config_path = 'config'
    boto_file_name = 'gsutil-service-account.boto'
    boto_file_path = f'{config_path}/{boto_file_name}'
    json_file_path = f'{config_path}/leo-etl-staging-ae90664302cb.json'

    # gsutil
    # export environment
    # os.environ['BOTO_PATH'] = boto_file_path
    print('.boto path:', boto_file_path)

    # get file info
    # info_day = datetime(year=2021, month=1, day=4)
    info_df = get_objects_info(
        bucket_name='staging-datasource',
        day=datetime(year=2021, month=1, day=19),
        step='crawl',
        use_boto=False,
        json_config_path=json_file_path
    )
    print(info_df.shape)
    print(info_df.head())

    # download batches in in info_df
    print(f'Number of empty files: {info_df[info_df.file_size.isin([0, 1])].shape[0]}')
    batches_to_read = info_df[  # only get batches with at least 1 non-empty file
        ~info_df.file_size.isin([0, 1])
    ]['batch_path'].unique().tolist() # get unique batch path
    print(f'Number of batches to read: {len(batches_to_read)}')

    # read into dataframe
    df_dict = {}
    begin_time = datetime.now()
    i = 0
    for batch in batches_to_read:
        df_dict['/'.join(batch.split('/')[3:5])] = download_gcs_to_dataframe(
            source_path=batch,
            destination_path='../data/',
            json_config_path=json_file_path,
            use_boto=False,
            num_threads=4,
            num_processes=12,
            delete=True
        )
        i += 1
        print(f'{datetime.now() - begin_time} elapsed - {i}/{len(batches_to_read)} - read from {batch}')
    print(df_dict.keys())
    print(df_dict[list(df_dict.keys())[0]].head())

