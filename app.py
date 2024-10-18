import os
from langchain.llms import OpenAI
from langchain.agents import initialize_agent, Tool
from langchain.agents import AgentType
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from flask import Flask, request, jsonify, render_template, session
from flask_session import Session
import logging

os.environ['PYSPARK_PYTHON'] = os.environ.get('PYSPARK_PYTHON', 'python')
os.environ['PYSPARK_DRIVER_PYTHON'] = os.environ.get('PYSPARK_DRIVER_PYTHON', 'python')
# Load environment variables
load_dotenv()

# Set OpenAI API Key
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
if OPENAI_API_KEY is None:
    raise ValueError("Please set your OpenAI API key in the .env file.")

# Initialize the LLM
llm = OpenAI(temperature=0, openai_api_key=OPENAI_API_KEY)

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def data_management_system(input_text: str) -> str:
    import os
    from pyspark.sql import SparkSession

    # Set environment variables for Spark
    os.environ['PYSPARK_PYTHON'] = os.environ.get('PYSPARK_PYTHON', 'python')
    os.environ['PYSPARK_DRIVER_PYTHON'] = os.environ.get('PYSPARK_DRIVER_PYTHON', 'python')

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("DataManagementSystem") \
        .getOrCreate()

    # Connect to Elasticsearch
    es = Elasticsearch(["http://localhost:9200"])

    try:
        logger.debug(f"Received input text: {input_text}")

        # Extract order_id and timestamp from input_text
        import re
        order_id_match = re.search(r'order_id (\d+)', input_text)
        timestamp_match = re.search(r'at (\d+ (AM|PM) on \d{2}-\d{2}-\d{4})', input_text)
        order_id = order_id_match.group(1) if order_id_match else None
        timestamp = timestamp_match.group(1) if timestamp_match else None

        logger.debug(f"Extracted order_id: {order_id}, timestamp: {timestamp}")

        # Construct Elasticsearch query
        s = Search(using=es, index='logs')
        if order_id:
            s = s.filter('term', order_id=order_id)
        if timestamp:
            # Convert timestamp to ISO format
            from datetime import datetime
            timestamp_dt = datetime.strptime(timestamp, '%I %p on %d-%m-%Y')
            timestamp_iso = timestamp_dt.isoformat()
            s = s.filter('range', timestamp={'lte': timestamp_iso})

        logger.debug(f"Elasticsearch query: {s.to_dict()}")

        # Execute query
        response = s.execute()
        logs = [hit.to_dict() for hit in response]

        logger.debug(f"Retrieved {len(logs)} logs from Elasticsearch")

        if not logs:
            spark.stop()
            return 'No relevant logs found.'

        # Create DataFrame from logs
        df = spark.createDataFrame(logs)
        logger.debug("Created Spark DataFrame")
        df.printSchema()
        df.show()

        # Data Processing using Spark
        df_filtered = df.filter(df.status.isNotNull())
        df_transformed = df_filtered.select('appName', 'status', 'timestamp', 'errorDetails')
        df_correlated = df_transformed.orderBy('timestamp')

        logger.debug("Processed DataFrame:")
        df_correlated.show()

        # Collect results with a limit
        processed_logs = df_correlated.limit(50).collect()
        logger.debug(f"Processed {len(processed_logs)} logs")

        # Convert processed logs to string
        processed_logs_str = '\n'.join([str(row.asDict()) for row in processed_logs])

        spark.stop()

        return processed_logs_str
    except Exception as e:
        spark.stop()
        logger.error(f'Error in Data Management System Tool: {str(e)}', exc_info=True)
        return 'An error occurred while processing your request.'

# Define the tool
dms_tool = Tool(
    name="Data Management System",
    func=data_management_system,
    description="Useful for querying the data management system for order status or error diagnosis. Provide the order_id and timestamp in your query."
)

# List of tools the agent can use
tools = [dms_tool]

# Initialize the agent
agent = initialize_agent(
    tools=tools,
    llm=llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=False
)

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')

app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_history')
def get_history():
    history = session.get('history', [])
    return jsonify({'history': history})

@app.route('/chat', methods=['POST'])
def chat():
    user_message = request.json.get('message')
    # Append user message to session history
    history = session.get('history', [])
    history.append({'sender': 'user', 'text': user_message})
    session['history'] = history

    response = agent.run(user_message)

    # Append agent response to session history
    history.append({'sender': 'agent', 'text': response})
    session['history'] = history

    return jsonify({'response': response})

if __name__ == '__main__':
    app.run(debug=True)
