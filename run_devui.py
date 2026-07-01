import sys
import logging
from agent_framework.devui import serve
from workflow import create_etl_workflow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DevUI-Runner")

def main():
    logger.info("Initializing ETL Pipeline Workflow...")
    workflow = create_etl_workflow()
    
    logger.info("Starting DevUI debug server on port 8080...")
    logger.info("Access the visual debug interface at http://localhost:8081")
    
    # Serve the workflow
    serve(entities=[workflow], port=8081)

if __name__ == "__main__":
    main()
