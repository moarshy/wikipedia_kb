import logging
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Any
from naptha_sdk.client.node import Node

logger = logging.getLogger(__name__)

file_path = Path(__file__).parent / "data" / "wikipedia_kb.parquet"

async def run(kb_run: Dict[str, Any], *args, **kwargs):
    """
    Run the Wikipedia Knowledge Base deployment
    Args:
        kb_run: Knowledge base run configuration containing deployment details
    """
    logger.info(f"Running Wikipedia Knowledge Base with deployment: {kb_run}")
    
    # Extract configuration
    kb_config = kb_run.kb_deployment.kb_config
    if not isinstance(kb_config, dict):
        kb_config = kb_config.model_dump()
    table_name = kb_config['table_name']
    schema = kb_config['schema']

    # Create a node client
    node_client = Node(kb_run.kb_deployment.kb_node_url)
    
    # Read the parquet file
    df = pd.read_parquet(file_path)
    
    try:
        # Create the table
        logger.info(f"Creating table {table_name}")
        await node_client.create_dynamic_table(table_name, schema)
        
        # Convert DataFrame rows to dictionaries and add them to the table
        logger.info("Adding rows to table")
        for _, row in tqdm(df.iterrows(), total=len(df)):
            row_data = {
                "id": int(row['id']),
                "url": str(row['url']),
                "title": str(row['title']),
                "text": str(row['text'])
            }
            
            await node_client.add_dynamic_row(table_name, row_data)
        
        logger.info(f"Successfully populated {table_name} table with {len(df)} rows")

        
    except Exception as e:
        raise