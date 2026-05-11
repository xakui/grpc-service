"""
Streamlit Web UI for gRPC Service Testing - Single Turn
"""
import streamlit as st
import json
import sys
import os

# Add parent directory to path to import grpc_service
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from grpc_service import GrpcService, grpc_config
from google.protobuf.json_format import ParseDict, MessageToDict
from cerence.cloudservices.api.common.v1 import interaction_history_pb2
import base64

st.set_page_config(
    page_title="gRPC Service Tester",
    page_icon="🔧",
    layout="wide"
)

st.title("🔧 gRPC Service Tester (Single Turn)")
st.markdown("Test the Cerence gRPC text query service")

# Query input
st.header("💬 Query Input")
utterance = st.text_area(
    "Enter your query:",
    placeholder="Type your text query here...",
    height=100
)

# Optional interaction history (JSON format)
st.header("� Interaction History (Optional)")
st.markdown("Paste JSON interaction history if needed for context")
interaction_history_json = st.text_area(
    "Interaction History JSON:",
    placeholder='[{"query": "...", "response": "..."}]',
    height=150
)

# Send button
if st.button("🚀 Send Query") and utterance.strip():
    with st.spinner("Initializing service and processing query..."):
        try:
            # Parse interaction history if provided
            interaction_history = []
            if interaction_history_json.strip():
                try:
                    ih_data = json.loads(interaction_history_json)
                    interaction_history = [
                        ParseDict(ih, interaction_history_pb2.InteractionHistory())
                        for ih in ih_data
                    ]
                except json.JSONDecodeError:
                    st.error("❌ Invalid JSON format for interaction history")
                    st.stop()
            
            # Auto-initialize service with default config
            grpc_service = GrpcService()
            
            # Make the query
            result = grpc_service.query(
                utterance,
                interaction_history,
                None  # No session data for single-turn
            )
            
            st.success("✅ Query processed successfully!")
            
            # Display response
            st.header("📊 Response")
            st.json(result)
            
        except Exception as e:
            st.error(f"❌ Query failed: {str(e)}")
            import traceback
            st.text(traceback.format_exc())
