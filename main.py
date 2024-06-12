from dotenv import load_dotenv
load_dotenv()


import os
import asyncio
from sqlalchemy import create_engine, text
from utils.database import Database
from datetime import date, timedelta


class DataExtractor:
    def __init__(self, project_number):
        self.project_number = project_number

    async def extract_data(self):
        # Simulate data extraction
        return f"Data extracted for project {self.project_number}"

class DataImporter:
    def __init__(self, project_number):
        self.project_number = project_number

    def import_data(self, data):
        # Logic to import data
        return f"Data imported for project {self.project_number}: {data}"


# Create SQLAlchemy async engine
engine = create_engine(Database().uri, echo=True, future=True)

# Define your query
query = text(f"{os.environ['active_projects_query']} '{date.today() - timedelta(1)}'")


async def dispatch_task(data_extractor, data_importer):
    # Define your task dispatching logic here
    extracted_data = data_extractor.extract_data()
    imported_data = data_importer.import_data(extracted_data)

    print(imported_data)


def fetch_projects():
    # Execute the query asynchronously
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    return rows


async def main():
    # Fetch data from the database
    rows = fetch_projects()

    # Dispatch tasks based on fetched data
    tasks = [dispatch_task(DataExtractor(row[0]), DataImporter(row[0])) for row in rows]
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
else:
    quit("\n\nThis script is meant to be run directly.")
