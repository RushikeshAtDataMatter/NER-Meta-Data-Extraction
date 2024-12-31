from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from metadata.schema.new_metadata import newMetaData,Base
from sqlalchemy.orm import session , sessionmaker 
from metadata.database.database import engine ,SessionLocal 
from metadata.schema.new_metadata import newMetaData , Base
import pandas as pd
from metadata.helper.handeler import analyze_csv_data
from metadata.helper.storage import store_metadata_in_db
from ydata_profiling import ProfileReport
import json
import os



# Create tables in the database
Base.metadata.create_all(bind=engine)

# Initialize FastAPI
app = FastAPI()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from metadata.database.database import engine, SessionLocal
from metadata.schema.new_metadata import newMetaData, Base
import io

# Create tables in the database
Base.metadata.create_all(bind=engine)

# Initialize FastAPI
app = FastAPI()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get('/metadata')
def get_metadata(db: Session = Depends(get_db)):
    metadata = db.query(newMetaData).all()
    return metadata

@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...), country_code: str = Query(...), db: Session = Depends(get_db)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file format. Only CSV files are allowed.")

    try:
        # Read the CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Generate the profiling report
        profiler = ProfileReport(df)
        report_path = 'D:/FastAPI/report.json'
        profiler.to_file(report_path)

        # Load the generated JSON report
        if os.path.exists(report_path):
            try:
                with open(report_path, 'r') as json_file:
                    report_data = json.load(json_file)
            except json.JSONDecodeError:
                raise HTTPException(status_code=500, detail="The JSON report is invalid or corrupted.")
        else:
            raise HTTPException(status_code=500, detail="The JSON report file could not be found.")

        # Analyze the CSV data
        results = analyze_csv_data(df, report_data, country_code)

        # Store the results in the database
        store_metadata_in_db(results, db)

        return JSONResponse({"message": "File processed and metadata stored successfully."}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")
