# gRPC Service Web UI

A Streamlit-based web interface for testing the Cerence gRPC text query service.

## Installation

1. Install dependencies:
```bash
pip install -r ../requirements.txt
```

2. Navigate to the webui directory:
```bash
cd webui
```

## Running the App

Start the Streamlit app:
```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

## Features

- **Configuration Panel**: Set OAuth credentials and connection parameters
- **Query Input**: Send text queries to the gRPC service
- **Conversation History**: View multi-turn conversation with the service
- **Session Details**: Inspect interaction history and session data
- **Real-time Responses**: View JSON-formatted responses from the service

## Usage

1. Open the sidebar and configure the connection settings (default values are pre-filled)
2. Click "🔄 Initialize Service" to connect to the gRPC service
3. Enter your query in the text area
4. Click "🚀 Send Query" to submit your request
5. View the response in the right panel
6. Continue the conversation by sending more queries
7. Use "🗑️ Clear Conversation" to reset the session
