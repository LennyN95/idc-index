import os
import logging
import pandas as pd
import platform
import subprocess
import duckdb
import urllib.request
import re
import tempfile

logger = logging.getLogger(__name__)

idc_version = "v17"
release_version = "0.2.10"
aws_endpoint_url = "https://s3.amazonaws.com"
gcp_endpoint_url = "https://storage.googleapis.com"
latest_idc_index_csv_url = (
    "https://github.com/ImagingDataCommons/idc-index/releases/download/"
    + release_version
    + "/idc_index.csv.zip"
)


class IDCClient:
    
    def __init__(self, data=None):
        
        # reset data store
        if data is None:
            self.reset()
        else:
            self._setDataStore(data)

        # set the s5cmd path
        self.s5cmd_path = self._get_s5cmd_path()

        # Print after successful reading of index
        logger.debug("Successfully read the index and located s5cmd.")

    # ---==== UTILITY, STATE AND HOUSEKEEPING METHODS ====---

    def _setDataStore(self, data: pd.DataFrame) -> None:
        assert isinstance(data, pd.DataFrame), "data must be a pandas dataframe"

        # update / set the internal data store
        self.data = data

        # refresh summary
        self.collection_summary = self.index.groupby("collection_id").agg(
            {"Modality": pd.Series.unique, "series_size_MB": "sum"}
        )

    def _get_s5cmd_path(self) -> None:
        
        s5cmdPath = None
        system = platform.system()
        
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # construct path based on os
        if system == "Windows":
            s5cmdPath = os.path.join(current_dir, "s5cmd.exe")
        elif system == "Darwin":
            s5cmdPath = os.path.join(current_dir, "s5cmd")
        else:
            s5cmdPath = os.path.join(current_dir, "s5cmd")

        # check if s5cmd executable exists
        if not os.path.exists(s5cmdPath):
            try:
                subprocess.run(["s5cmd", "-s-help"], capture_output=False, text=False, stdout=subprocess.DEVNULL,)
                s5cmdPath = "s5cmd"
                
            except:
                logger.fatal("s5cmd executable not found. Please install s5cmd from https://github.com/peak/s5cmd#installation")
                raise ValueError
            
        # return path
        return s5cmdPath

    def reset(self, refresh: bool = False):
        """
        Initiate the internal data store with the idc index data.
        If data is not available locally or refresh is specified, the latest release will be downloaded.

        Args:
          refresh: if True, the latest release will be downloaded even if data is available locally
        """

        # load data
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, "idc_index.csv.zip")
        if not os.path.exists(file_path) or refresh:
            
            # print
            if not not os.path.exists(file_path):
                logger.warning(
                    "Index file not found. Downloading latest version of the index file. This will take a minute or so."
                )
            elif refresh:
                logger.warning(
                    "Refreshing index file. Downloading latest version of the index file. This will take a minute or so."
                )

            # download
            urllib.request.urlretrieve(latest_idc_index_csv_url, file_path)

            # print
            logger.warning("Index file downloaded.")

        # read and format data
        data = pd.read_csv(file_path, dtype=str, encoding="utf-8")
        data = data.astype(str).replace("nan", "")
        data["series_size_MB"] = data["series_size_MB"].astype(float)

        # set data
        self._setDataStore(data)
        
    # ---==== FILTER AND QUERY METHODS ====---        

    def sql(self, sql_query, context=None, in_place=False) -> 'IDCClient':
        """Execute SQL query against the table in the index using duckdb.

        Args:
            sql_query: string containing the SQL query to execute. The table name to use in the FROM clause is 'index' (without quotes).
            context: pandas dataframe to use as context for the query. If not specified, the internal index will be used.

        Returns:
            pandas dataframe containing the results of the query

        Raises:
            any exception that duckdb.query() raises
        """

        assert context is None or isinstance(context, pd.DataFrame), \
            "context if specified must be a pandas dataframe"
        
        # specify duckdb context (`index`` will be accessible as table in the query)
        index = self.index if context is None else context
        
        # execute query and return data as pandas dataframe
        data = duckdb.query(sql_query).to_df()
        
        # update datastore or return nes instance of IDCClient (depending on in_place)
        if in_place:
            self._setDataStore(data=data)
            return self
        else:
            return IDCClient(data=data)
     
    def get(self, collectionId=None, patientId=None, studyInstanceUID=None, seriesInstanceUID=None, in_place=False) -> 'IDCClient':
        """Filter the index based on the specified criteria and return a new instance of IDCClient or update the current instance of IDCClient (depending on in_place). this method provides a simple query interface. You can specify any combination of arguments to filter the idc index for. When multiple arguments are specified, they will be combined with an AND conjunction. 
        For more complex queries, you may use the sql_query method instead.

        Args:
            collectionId (str, optional): _description_. Defaults to None.
            patientId (str, optional): _description_. Defaults to None.
            studyInstanceUID (str, optional): _description_. Defaults to None.
            seriesInstanceUID (str, optional): _description_. Defaults to None.
            in_place (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """

        # at least one argument must be specified
        assert (
            collectionId is not None
            or patientId is not None
            or studyInstanceUID is not None
            or seriesInstanceUID is not None
        ), "at least one argument must be specified"

        # construct sql base query
        query = f'''
            SELECT
            *
            FROM
                index
        '''

        # construct where clause
        where_clause = []

        if collectionId is not None:
            where_clause.append(f"collection_id='{collectionId}'")

        if patientId is not None:
            where_clause.append(f"PatientID='{patientId}'")

        if studyInstanceUID is not None:
            where_clause.append(f"StudyInstanceUID='{studyInstanceUID}'")

        if seriesInstanceUID is not None:
            where_clause.append(f"SeriesInstanceUID='{seriesInstanceUID}'")

        # append where clause to query
        if len(where_clause) > 0:
            query += f"WHERE {' AND '.join(where_clause)}"

        # execute query
        subset = self.sql(query)

        # update datastore or return nes instance of IDCClient (depending on in_place)
        if in_place:
            self._setDataStore(data=subset)
            return self
        else:
            return IDCClient(data=subset)
        
    # ---==== DATA RETRIEVAL METHODS ====---
    
    def download(self) -> None:
        
        # estimate download space
        
        # shall we stop / print a warning if the download size is too large / exceeds availabel disk space (if we can get that) or exceeds a specified threshold?
        
        # download
        # --> use download utility outsoured to a separate utils module
        # --> whe should support various download options (e.g. s5cmd, aws cli, gcp cli, wget etc.)
        # --> we should have those download engines beeing installed as complemnetary packages (e.g., pip install idc_index[s5cmd])
        
        pass
    
    def raw(self) -> pd.DataFrame:
        # NOTE: `pandas`, `table`, `data` might be better names...
        
        return self.data
    
    def summarize() -> pd.DataFrame:
