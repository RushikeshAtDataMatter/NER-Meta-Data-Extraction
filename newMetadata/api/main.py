from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from metadata.schema.new_metadata import newMetaData, Base
from metadata.database.database import engine, SessionLocal
from sqlalchemy.orm import Session
import pandas as pd
from ydata_profiling import ProfileReport
import io
import os
import json
from newMetadata.helper.profile import extract_and_store_metadata

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
    """Fetch all metadata from the database."""
    try:
        metadata = db.query(newMetaData).all()
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metadata: {str(e)}")

@app.post("/upload_csv")
async def upload_csv(
    file: UploadFile = File(...),
    country_code: str = Query(...),
    db: Session = Depends(get_db)
):
    """Upload and process a CSV file."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file format. Only CSV files are allowed.")

    try:
        # Read the CSV file
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))

        # Generate the profiling report
        profiler = ProfileReport(df, minimal=True)  # Use minimal=True for performance
        report_path = 'report.json'
        profiler.to_file(report_path)

        # Call the function to extract and store metadata
        extract_and_store_metadata(report_path, db, df)

        # Cleanup the generated report file after processing
        if os.path.exists(report_path):
            os.remove(report_path)

        return JSONResponse(content={"message": "File processed and metadata stored successfully."}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

