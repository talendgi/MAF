import os
import logging
from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass
from dotenv import load_dotenv
from matplotlib.pylab import record
import mysql.connector
from agent_framework import Executor, WorkflowContext, handler, WorkflowBuilder
from streamlit import columns

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETLWorkflow")


@dataclass
class ConnectionConfig:
    """Immutable configuration for database connections."""
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    account: Optional[str] = None
    warehouse: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None


class DatabaseConnector(ABC):
    """
    Abstract interface for database operations.
    """
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish database connection."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results."""
        pass
    
    @abstractmethod
    async def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information."""
        pass


# IMPLEMENTATIONS 

class MySQLConnector(DatabaseConnector):
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connection = None
        self._connected = False

    async def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port or 3306,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            self._connected = True
            logger.info(f"Connected to MySQL: {self.config.host}:{self.config.port}/{self.config.database}")
            return True
        except Exception as e:
            logger.error(f"MySQL connection failed: {e}")
            raise

    async def disconnect(self) -> None:
        if self.connection and self._connected:
            self.connection.close()
            self._connected = False

    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        if not self._connected:
            raise RuntimeError("Not connected to MySQL")

        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            if cursor.description is None:
                self.connection.commit()
                return [{"rows_affected": cursor.rowcount}]
            return cursor.fetchall()
        finally:
            cursor.close()
    async def get_schema(self, table_name: str) -> Dict[str, Any]:
        query = f"""
            SELECT UPPER(COLUMN_NAME) AS COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.config.database}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        return await self.execute_query(query)


class SnowflakeConnector(DatabaseConnector):
    """Snowflake database connector implementation."""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connection = None
        self._connected = False
    
    async def connect(self) -> bool:
        try:
            import snowflake.connector
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                schema=self.config.schema,
                warehouse=self.config.warehouse,
                role=self.config.role
            )
            self._connected = True
            logger.info(f"✅ Connected to Snowflake: {self.config.account}/{self.config.database}.{self.config.schema}")
            return True
        except Exception as e:
            logger.error(f"❌ Snowflake connection failed: {e}")
            raise
    
    async def disconnect(self) -> None:
        if self.connection and self._connected:
            self.connection.close()
            self._connected = False
            logger.info("🔌 Disconnected from Snowflake")
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        if not self._connected:
            raise RuntimeError("Not connected to Snowflake")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            
            # For INSERT/UPDATE/DELETE, return row count
            if cursor.description is None:
                return [{"rows_affected": cursor.rowcount}]
            
            # For SELECT queries, fetch results
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"📊 Snowflake query returned {len(results)} rows")
            return results
        finally:
            cursor.close()
    
    async def get_schema(self, table_name: str) -> Dict[str, Any]:
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.config.schema}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """
        return await self.execute_query(query)
    async def table_exists(self, table_name: str) -> bool:
        query = """
            SELECT COUNT(*) AS CNT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
        """
        rows = await self.execute_query(query, (self.config.schema, table_name.upper()))
        return rows[0]["CNT"] > 0

    async def create_table_if_not_exists(self, table_name: str, columns: List[Dict[str, Any]]) -> None:
        col_defs = []
        for col in columns:
            name = col["COLUMN_NAME"].upper()
            dtype = col["DATA_TYPE"]
            nullable = col.get("IS_NULLABLE", "YES")
            null_sql = "NULL" if nullable == "YES" else "NOT NULL"
            col_defs.append(f'"{name}" {dtype} {null_sql}')

        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        await self.execute_query(create_sql)


# FACTORY 

class ConnectorFactory:
    """Factory for creating database connectors. Easy to extend for new databases."""
    
    @staticmethod
    def create_mysql_from_env() -> MySQLConnector:
        """Create MySQL connector from environment variables."""
        config = ConnectionConfig(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE")
        )
        return MySQLConnector(config)
    
    @staticmethod
    def create_snowflake_from_env() -> SnowflakeConnector:
        """Create Snowflake connector from environment variables."""
        config = ConnectionConfig(
            account=os.getenv("SF_ACCOUNT"),
            user=os.getenv("SF_USER"),
            password=os.getenv("SF_PASSWORD"),
            database=os.getenv("SF_DATABASE"),
            schema=os.getenv("SF_SCHEMA"),
            warehouse=os.getenv("SF_WAREHOUSE"),
            role=os.getenv("SF_ROLE")
        )
        return SnowflakeConnector(config)
    
    # to add more factories:
    # @staticmethod
    # def create_postgres_from_env() -> PostgresConnector:
    #     ...


# EXECUTORS

class ExtractExecutor(Executor):
    """Extracts data from MySQL source."""
    
    def __init__(self, id: str, source_connector: DatabaseConnector, source_table: str):
        super().__init__(id=id)
        self.source_connector = source_connector
        self.source_table = source_table
    
    @handler
    async def process(self,input_data: Dict[str, Any],ctx: WorkflowContext[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:

        logger.info(f"[{self.id}] 🔄 Extracting data from {self.source_table}...")
        
        try:
            await self.source_connector.connect()
            
            # Extract all data from source table
            query = f"SELECT * FROM {self.source_table}"
            records = await self.source_connector.execute_query(query)
            
            await self.source_connector.disconnect()
            
            # Store metadata in workflow context
            ctx.set_state("source_table", self.source_table)
            ctx.set_state("extracted_count", len(records))
            
            payload = {
                "records": records,
                "source_table": self.source_table,
                "record_count": len(records)
            }
            
            logger.info(f"[{self.id}] ✅ Extracted {len(records)} records")
            await ctx.yield_output(payload)
            await ctx.send_message(payload)
            
            return payload
            
        except Exception as e:
            logger.error(f"[{self.id}] ❌ Extraction failed: {e}")
            await self.source_connector.disconnect()
            raise

class TransformExecutor(Executor):
    """Transforms data (currently pass-through, can be extended)."""
    
    def __init__(self, id: str):
        super().__init__(id=id)
    
    @handler
    async def process(self,input_data: Dict[str, Any],ctx: WorkflowContext[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:

        logger.info(f"[{self.id}] 🔄 Transforming data...")
        
        records = input_data.get("records", [])
        
        # Example transformation: Add a processing timestamp
        from datetime import datetime
        for record in records:
            record["processed_at"] = datetime.utcnow().isoformat()
        
        ctx.set_state("transformed_count", len(records))
        
        payload = {
            "records": records,
            "source_table": input_data.get("source_table"),
            "record_count": len(records)
        }
        
        logger.info(f"[{self.id}] ✅ Transformed {len(records)} records")
        await ctx.yield_output(payload)
        await ctx.send_message(payload)
        
        return payload

class CheckTableExistsExecutor(Executor):
    def __init__(self, id: str, dest_connector: SnowflakeConnector, dest_table: str):
        super().__init__(id=id)
        self.dest_connector = dest_connector
        self.dest_table = dest_table

    @handler
    async def process(self, input_data: Dict[str, Any], ctx: WorkflowContext[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"[{self.id}] Checking if table exists: {self.dest_table}")
        await self.dest_connector.connect()
        exists = await self.dest_connector.table_exists(self.dest_table)
        await self.dest_connector.disconnect()

        payload = {**input_data, "table_exists": exists}
        logger.info(f"[{self.id}] table_exists={exists}")
        await ctx.send_message(payload)
        return payload


class CreateTableExecutor(Executor):
    def __init__(self, id: str, dest_connector: SnowflakeConnector, dest_table: str, source_connector: MySQLConnector, source_table: str):
        super().__init__(id=id)
        self.dest_connector = dest_connector
        self.dest_table = dest_table
        self.source_connector = source_connector
        self.source_table = source_table

    @handler
    async def process(self, input_data: Dict[str, Any], ctx: WorkflowContext[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"[{self.id}] Creating table if not exists: {self.dest_table}")

        await self.source_connector.connect()
        source_schema = await self.source_connector.get_schema(self.source_table)
        await self.source_connector.disconnect()
        # including the PROCESSED_AT column
        source_schema.append({
            "COLUMN_NAME": "PROCESSED_AT",
            "DATA_TYPE": "TIMESTAMP_NTZ",
            "IS_NULLABLE": "YES"
        })

        await self.dest_connector.connect()
        await self.dest_connector.create_table_if_not_exists(self.dest_table, source_schema)
        await self.dest_connector.disconnect()

        payload = {**input_data, "table_created": True}
        await ctx.send_message(payload)
        return payload

class LoadExecutor(Executor):
    """Loads data into Snowflake destination."""
    
    def __init__(self, id: str, dest_connector: DatabaseConnector, dest_table: str):
        super().__init__(id=id)
        self.dest_connector = dest_connector
        self.dest_table = dest_table
    
    @handler
    async def process(self,input_data: Dict[str, Any],ctx: WorkflowContext[Dict[str, Any], Dict[str, Any]]) -> Dict[str, Any]:

        logger.info(f"[{self.id}] 🔄 Loading data into {self.dest_table}...")
        
        records = input_data.get("records", [])
        
        if not records:
            logger.warning(f"[{self.id}] No records to load")
            empty_result = {"status": "SKIPPED", "rows_loaded": 0}
            await ctx.yield_output(empty_result)
            return empty_result
        
        try:
            await self.dest_connector.connect()
            
            # Get first record to determine columns
            columns = [c.upper() for c in records[0].keys()]
            # columns = list(records[0].keys())
            columns_str = ", ".join(columns)
            
            # Create INSERT statement
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {self.dest_table} ({columns_str}) VALUES ({placeholders})"
            
            # Execute inserts
            rows_loaded = 0
            for record in records:
                normalized_record = {k.upper(): v for k, v in record.items()}
                values = tuple(normalized_record[col] for col in columns)
                rows_loaded += 1
                await self.dest_connector.execute_query(insert_query, values)
            
            await self.dest_connector.disconnect()
            
            # Store final metrics
            ctx.set_state("loaded_count", rows_loaded)
            
            report = {
                "status": "COMPLETED",
                "source_table": input_data.get("source_table"),
                "dest_table": self.dest_table,
                "rows_loaded": rows_loaded,
                "metrics": {
                    "extracted": ctx.get_state("extracted_count", 0),
                    "transformed": ctx.get_state("transformed_count", 0),
                    "loaded": rows_loaded,
                    "insert_query": insert_query
                }
            }
            logger.info(f"Insert query : {insert_query}")
            logger.info(f"[{self.id}] ✅ Successfully loaded {rows_loaded} records")
            await ctx.yield_output(report)
            
            return report
            
        except Exception as e:
            logger.error(f"[{self.id}] ❌ Load failed: {e}")
            logger.info(f"insert query : {insert_query}")
            await self.dest_connector.disconnect()
            raise



# WORKFLOW BUILDER

def create_etl_workflow(
    source_table: str = "blood_compatibility_lookup",
    dest_table: str = "blood_compatibility_lookup_el"
) -> Any:
    """
    Builds the ETL workflow: Extract → Transform → Load

    """
    logger.info("🏗️  Building ETL Workflow...")
    
    # Create connectors using factory
    mysql_connector = ConnectorFactory.create_mysql_from_env()
    snowflake_connector = ConnectorFactory.create_snowflake_from_env()
    
    # Create executors
    extract_exec = ExtractExecutor("Extract from Mysql", mysql_connector, source_table)
    transform_exec = TransformExecutor("Transform Data")
    check_exec = CheckTableExistsExecutor("Check if table exists", snowflake_connector, dest_table)
    create_exec = CreateTableExecutor("Create table", snowflake_connector, dest_table, mysql_connector, source_table)
    load_exec = LoadExecutor("Load into Snowflake", snowflake_connector, dest_table)
    
    # Build workflow graph
    builder = WorkflowBuilder(
        name="MySQL_to_Snowflake_ETL",
        description=f"ETL pipeline : {source_table} (MySQL) → {dest_table} (Snowflake)",
        start_executor=extract_exec,
        output_from=[load_exec],
        intermediate_output_from="all_other"
    )
    
    # Define execution flow
    builder.add_edge(extract_exec, transform_exec)
    builder.add_edge(transform_exec, check_exec)
    builder.add_edge(check_exec, load_exec, condition=lambda msg: msg.get("table_exists", False))
    builder.add_edge(check_exec, create_exec, condition=lambda msg: not msg.get("table_exists", False))
    builder.add_edge(create_exec, load_exec)
    
    logger.info("✅ Workflow built successfully")
    return builder.build()


# MAIN EXECUTION

if __name__ == "__main__":
    import asyncio
    
    async def run_etl():
        # Configure your tables here
        SOURCE_TABLE = "blood_compatibility_lookup"  # MySQL table
        DEST_TABLE = "blood_compatibility_lookup_el"  # Snowflake table
        
        print("=" * 60)
        print("🚀 Starting MySQL → Snowflake ETL Pipeline")
        print("=" * 60)
        
        # Create workflow
        workflow = create_etl_workflow(
            source_table=SOURCE_TABLE,
            dest_table=DEST_TABLE
        )
        
        # Run workflow
        print("\n📋 Running workflow...")
        result = await workflow.run({"job_id": 1, "trigger": "manual"})
        
        # Print results
        print("\n" + "=" * 60)
        print("📊 FINAL RESULTS")
        print("=" * 60)
        outputs = result.get_outputs()
        for output in outputs:
            print(f"\n✅ {output}")
        
        print("\n📈 INTERMEDIATE OUTPUTS")
        print("=" * 60)
        intermediate = result.get_intermediate_outputs()
        for i, output in enumerate(intermediate, 1):
            print(f"\nStep {i}: {output}")
    
    asyncio.run(run_etl())