import logging
import json
import random
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Any
from naptha_sdk.client.node import Node
from wikipedia_kb.schemas import InputSchema
logger = logging.getLogger(__name__)

file_path = Path(__file__).parent / "data" / "wikipedia_kb_sample.parquet"


class WikipediaKB:
    def __init__(self, kb_run: Dict[str, Any]):
        self.kb_run = kb_run
        self.kb_deployment = kb_run.kb_deployment
        self.kb_node_url = self.kb_deployment.kb_node_url
        self.kb_config = self.kb_deployment.kb_config

        if isinstance(self.kb_run.inputs, dict):
            self.input_schema = InputSchema(**self.kb_run.inputs)
        else:
            self.input_schema = InputSchema.model_validate(self.kb_run.inputs)

        self.mode = self.input_schema.mode
        self.query = self.input_schema.query

    async def run(self, *args, **kwargs):
        if self.mode == "init":
            return await self.init()
        elif self.mode == "query":
            return await self.run_query()
        elif self.mode == "add_data":
            return await self.add_data()
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
        
    async def init(self, *args, **kwargs):
        node_client = Node(self.kb_node_url)
        table_name = self.kb_config['table_name']
        schema = self.kb_config['schema']

        # Create the table
        logger.info(f"Creating table {table_name}")
        await node_client.create_table(table_name, schema)

        # Load the df
        df = pd.read_parquet(file_path)

        # Add rows to the table
        logger.info("Adding rows to table")
        for _, row in tqdm(df.iterrows(), total=len(df)):
            row_data = {
                "id": int(row['id']),
                "url": str(row['url']),
                "title": str(row['title']),
                "text": str(row['text'])
            }

            await node_client.add_row(table_name, row_data)
        
        logger.info(f"Successfully populated {table_name} table with {len(df)} rows")

        return {"status": "success", "message": f"Successfully populated {table_name} table with {len(df)} rows"}
    
    async def add_data(self, *args, **kwargs):
        node_client = Node(self.kb_node_url)
        table_name = self.kb_config['table_name']
        schema = self.kb_config['schema']

        # Add rows to the table
        data = json.loads(self.input_schema.data)

        logger.info(f"Adding {len(data)} rows to table {table_name}")
        for row in tqdm(data, total=len(data)):
            # if row has no id, generate a random one
            if 'id' not in row:
                row['id'] = random.randint(1, 1000000)

            # make sure title are not in the table
            if await node_client.query_table(table_name, condition={'title': row['title']}):
                logger.info(f"Title {row['title']} already exists in table {table_name}, skipping")
                continue

            await node_client.add_row(table_name, row)

        logger.info(f"Successfully added {len(data)} rows to table {table_name}")

        return {"status": "success", "message": f"Successfully added {len(data)} rows to table {table_name}"}

    async def run_query(self, *args, **kwargs):
        node_client = Node(self.kb_node_url)
        table_name = self.kb_config['table_name']
        schema = self.kb_config['schema']

        # Query the table
        logger.info(f"Querying table {table_name} with query: {self.query}")
        results = await node_client.query_table(table_name=table_name, condition={'title': self.query})
        logger.info(f"Query results: {results}")

        return {"status": "success", "message": f"Query results: {results}"}


async def run(kb_run: Dict[str, Any], *args, **kwargs):
    """
    Run the Wikipedia Knowledge Base deployment
    Args:
        kb_run: Knowledge base run configuration containing deployment details
    """
    wikipedia_kb = WikipediaKB(kb_run)
    return await wikipedia_kb.run()
