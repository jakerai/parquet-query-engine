# parquet-query-engine
Spring Boot sample application for querying and ingesting Parquet files using DuckDB

The REST API include in this sample allows querying and ingesting Parquet files using DuckDB. It provides endpoints to read Parquet files directly, ingest single or multiple files into DuckDB, and query ingested data with optional filters. The API supports configurable file sources from local filesystem or Amazon S3.

ENDPOINTS

1: Read Parquet File Directly from parquet file (No Ingestion)
   
   Example Http Request:

   GET: http://localhost:5000/parquet/{my-filename.parquet}?country={country}

   Example Response:

    {
	    "success": true,
	    "message": "Fetched data from DuckDB table successfully",
	    "status": 0,
	    "timestamp": "2026-02-03T11:57:08.641368200Z",
	    "requestId": "f235ae9a03ed3326f438270c6fd2f429",
	    "data": [
	        {
	            "id": 1,
	            "first_name": "Alice Smith",
	            "age": 51,
	            "country": "UK"
	        },
	        {
	            "id": 2,
	            "first_name": "Alice Johnson",
	            "age": 57,
	            "country": "UK"
	        }
	     ]
	}


2: Ingest Single File into DuckDB
   
   Example Http Request:
   
   POST: http://localhost:5000/parquet/ingest
   
   Request Body:
    
        {
          "payload": {
             "fileName": "sample.parquet"
           }
        }

   Example Response:
   
	   {
	    "success": true,
	    "message": "Parquet files ingested successfully",
	    "status": 0,
	    "timestamp": "2026-02-03T12:35:29.512258900Z",
	    "requestId": "87af714f5a970dc5fb5ecc0be0b4f656"
	   }
	    
    
3: Ingest All Files into DuckDB

   Example Http Request:
   
   POST: http://localhost:5000/parquet/ingest-files
   
   Example Response:

	    {
	    "success": true,
	    "message": "Parquet files ingested successfully",
	    "status": 0,
	    "timestamp": "2026-02-03T12:38:37.111202300Z",
	    "requestId": "8a1c17aef4a102174a61c501c208eacb"
	    }       



4: Query Ingested DuckDB Table

   Example Http Request:

   GET: http://localhost:5000/parquet/ingest?firstName={firstName}&country={country}
      
   Example Response:

		{
		    "success": true,
		    "message": "Fetched data from DuckDB table successfully",
		    "status": 0,
		    "timestamp": "2026-02-03T11:57:08.641368200Z",
		    "requestId": "f235ae9a03ed3326f438270c6fd2f429",
		    "data": [
		        {
		            "id": 1,
		            "first_name": "Alice Smith",
		            "age": 51,
		            "country": "US"
		        },
		        {
		            "id": 2,
		            "first_name": "Alice Johnson",
		            "age": 57,
		            "country": "UK"
		        }
		     ]
		}
