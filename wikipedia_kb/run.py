import logging
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from typing import Dict, Any
from naptha_sdk.client.node import Node

logger = logging.getLogger(__name__)

file_path = Path(__file__).parent / "data" / "wikipedia_kb_sample.parquet"

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
        await node_client.create_table(table_name, schema)
        
        # Convert DataFrame rows to dictionaries and add them to the table
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

        
    except Exception as e:
        raise


if __name__ == "__main__":
    from naptha_sdk.client.node import Node
    import asyncio
    import json
    from pydantic import BaseModel

    with open("./wikipedia_kb/configs/knowledge_base_deployment.json", "r") as f:
        kb_deployment = json.load(f)
    
    kb_run = {
        "kb_deployment": kb_deployment
    }

    class KBDeployment(BaseModel):
        kb_node_url: str
        kb_config: Dict[str, Any]
    class KBRun(BaseModel):
        kb_deployment: KBDeployment

    kb_run = KBRun(**kb_run)

    print(kb_run)

    asyncio.run(run(kb_run))