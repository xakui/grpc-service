"""
# @Time    : 04/14/2026
# @Author  : kui.xiao
# @Description

"""
import json
import base64

import requests
import grpc
from google.protobuf.json_format import MessageToDict, ParseDict
from cerence.cloudservices.api.services.text.v1.text_api_pb2_grpc import TextQueryServiceStub
import cerence.cloudservices.api.services.text.v1.text_api_pb2 as text_api
import cerence.cloudservices.api.services.text.v1.text_context_pb2 as text_context
from cerence.cloudservices.domain.results.dialog.v1 import domain_results_pb2 as dialog_results_pb2
from cerence.cloudservices.domain.results.media.v1 import domain_results_pb2 as media_results_pb2
from cerence.cloudservices.domain.results.news.v1 import domain_results_pb2 as news_results_pb2
from cerence.cloudservices.domain.results.nlu.v1 import domain_results_pb2 as nlu_results_pb2
from cerence.cloudservices.domain.results.pa.v1 import domain_results_pb2 as pa_results_pb2
from cerence.cloudservices.domain.results.stock.v1 import domain_results_pb2 as stock_results_pb2
from cerence.cloudservices.domain.results.ude.v1 import domain_results_pb2 as ude_results_pb2
from cerence.cloudservices.domain.results.communication.v1 import domain_results_pb2 as communication_results_pb2
from cerence.cloudservices.domain.results.creative_work.v1 import domain_results_pb2 as creative_work_results_pb2
from cerence.cloudservices.domain.results.sing.v1 import domain_results_pb2 as sing_results_pb2
from cerence.cloudservices.domain.results.weather.v1 import domain_results_pb2 as weather_results_pb2
from google.type import latlng_pb2
from google.protobuf import wrappers_pb2
from cerence.cloudservices.api.common.v1 import interaction_history_pb2
from collections import namedtuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from functools import lru_cache
import asyncio
import datetime
from loguru import logger

grpc_config = {
    "cerence_host": "cerence-ref-uat-eng-usa.prod.na.oc.cerenceapi.com",
    "cerence_port": 443,
    "oauth_token_url": "https://oauth-cerence-ref.prod.na.oc.cerenceapi.com/oauth2/token",
    "oauth_token_fallback_urls": [
        "https://auth.prod.na.oc.cerence.net/oauth2/token"
    ],
    "oauth_client_id": "cerence-ref-p0001",
    "oauth_client_secret": "6cfa503fb17e6c21247a62cf5350c7fe5d32b982a51649d3b499870ec9b2ce7d",
    "oauth_scope": "access:speech",
    "session_test_request": "mib4perplexityllmpoc-integration-no-obfuscation",
    "session_convmgr_routing": "ccb"
}

#
# grpc_config = {
#     "cerence_host": "cerence-ref-uat-eng-usa.prod.na.oc.cerenceapi.com:443",
#     "oauth_host": "https://oauth-cerence-ref.prod.na.oc.cerenceapi.com",
#     "oauth_client_id": "cerence-ref-p0001",
#     "oauth_client_secret": "6cfa503fb17e6c21247a62cf5350c7fe5d32b982a51649d3b499870ec9b2ce7d",
#     "oauth_scope": "access:speech"
# }

_ClientCallDetails = namedtuple(
    "_ClientCallDetails",
    ["method", "timeout", "metadata", "credentials", "wait_for_ready", "compression"]
)


class HeaderClientInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    def __init__(self, metadata_provider):
        self._metadata_provider = metadata_provider

    @staticmethod
    def _copy_call_details(client_call_details, metadata):
        return _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=getattr(client_call_details, "wait_for_ready", None),
            compression=getattr(client_call_details, "compression", None),
        )

    def _intercept(self, continuation, client_call_details, request):
        metadata = list(client_call_details.metadata or [])
        metadata.extend(self._metadata_provider())
        return continuation(self._copy_call_details(client_call_details, metadata), request)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

class GrpcClient:
    TYPE_REGISTRY = {
        dialog_results_pb2.PlugAndPlayDialogResponse.DESCRIPTOR.full_name: dialog_results_pb2.PlugAndPlayDialogResponse,
        dialog_results_pb2.SelfServiceDialogResponse.DESCRIPTOR.full_name: dialog_results_pb2.SelfServiceDialogResponse,
        news_results_pb2.NewsResponse.DESCRIPTOR.full_name: news_results_pb2.NewsResponse,
        nlu_results_pb2.NluOnlyResponse.DESCRIPTOR.full_name: nlu_results_pb2.NluOnlyResponse,
        pa_results_pb2.PersonalAssistantResponse.DESCRIPTOR.full_name: pa_results_pb2.PersonalAssistantResponse,
        stock_results_pb2.StockResponse.DESCRIPTOR.full_name: stock_results_pb2.StockResponse,
        weather_results_pb2.SkiConditionsResponse.DESCRIPTOR.full_name: weather_results_pb2.SkiConditionsResponse,
        weather_results_pb2.WeatherOverviewResponse.DESCRIPTOR.full_name: weather_results_pb2.WeatherOverviewResponse,
        weather_results_pb2.WeatherSummaryResponse.DESCRIPTOR.full_name: weather_results_pb2.WeatherSummaryResponse,
        ude_results_pb2.UdeResponse.DESCRIPTOR.full_name: ude_results_pb2.UdeResponse,
        ude_results_pb2.TrafficResponse.DESCRIPTOR.full_name: ude_results_pb2.TrafficResponse,
        media_results_pb2.MediaResponse.DESCRIPTOR.full_name: media_results_pb2.MediaResponse,
        communication_results_pb2.CommunicationResponse.DESCRIPTOR.full_name: communication_results_pb2.CommunicationResponse,
        creative_work_results_pb2.CreativeWorkResponse.DESCRIPTOR.full_name: creative_work_results_pb2.CreativeWorkResponse,
        sing_results_pb2.SingResponse.DESCRIPTOR.full_name: sing_results_pb2.SingResponse,
        dialog_results_pb2.PlugAndPlayDialogStreamingResponseStart.DESCRIPTOR.full_name: dialog_results_pb2.PlugAndPlayDialogStreamingResponseStart,
    }

    def __init__(
        self,
        cerence_host,
        cerence_port,
        oauth_token_url,
        oauth_client_id,
        oauth_client_secret,
        oauth_scope,
        oauth_token_fallback_urls=None,
        session_test_request=None,
        session_convmgr_routing=None,
    ):
        self.cerence_host = cerence_host
        self.cerence_port = cerence_port
        self.oauth_token_url = oauth_token_url
        self.oauth_token_fallback_urls = oauth_token_fallback_urls or []
        self.oauth_client_id = oauth_client_id
        self.oauth_client_secret = oauth_client_secret
        self.oauth_scope = oauth_scope
        self.session_test_request = session_test_request
        self.session_convmgr_routing = session_convmgr_routing

        self.http_session = self._build_http_session()
        self.oauth_token = None
        self.token_expiry = None
        self.issue_oauth_token()

        address = f"{self.cerence_host}:{self.cerence_port}"
        if self.cerence_port == 443:
            channel = grpc.secure_channel(address, grpc.ssl_channel_credentials())
        else:
            channel = grpc.insecure_channel(address)
        header_interceptor = HeaderClientInterceptor(self._build_metadata)
        self.channel = grpc.intercept_channel(channel, header_interceptor)
        self.client = TextQueryServiceStub(self.channel)

    @staticmethod
    def _build_http_session():
        session = requests.Session()
        retry_policy = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.8,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_policy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def issue_oauth_token(self):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.oauth_client_id,
            'client_secret': self.oauth_client_secret,
            'scope': self.oauth_scope
        }
        oauth_urls = [self.oauth_token_url, *self.oauth_token_fallback_urls]
        errors = []
        response = None

        for oauth_url in oauth_urls:
            try:
                response = self.http_session.post(
                    oauth_url,
                    data=payload,
                    timeout=(5, 20)
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as exc:
                if len(oauth_urls) > 1:
                    logger.warning(f"OAuth token request failed for {oauth_url}: {exc}")
                else:
                    logger.error(f"OAuth token request failed for {oauth_url}: {exc}")
                errors.append(f"{oauth_url} -> {exc}")
                response = None

        if response is None:
            details = "; ".join(errors)
            raise RuntimeError(
                "Failed to get OAuth token after retries across all endpoints. "
                f"Details: {details}. "
                "Please check DNS/VPN/proxy/TLS trust settings."
            )

        token_data = response.json()
        self.oauth_token = token_data['access_token']
        self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=token_data.get('expires_in', 7200))

    def _ensure_token_validity(self):
        if not self.oauth_token or datetime.datetime.now() >= self.token_expiry:
            self.issue_oauth_token()

    def _build_metadata(self):
        self._ensure_token_validity()

        metadata = [
            ('authorization', f'bearer {self.oauth_token}')
        ]
        if self.session_test_request:
            metadata.append(('cerence-session-test-request', self.session_test_request))
        if self.session_convmgr_routing:
            metadata.append(('cerence-session-convmgr-routing', self.session_convmgr_routing))
        return metadata

    def _invoke_grpc(self, utterance, interaction_history, session_data=None):
        self._ensure_token_validity()

        # req = text_api.TextQueryRequest(
        #     input_text=utterance,
        #     user_id='llm-team-demo',
        #     device_id='llm-team-demo-device'
        # )
        interaction_history=interaction_history
        req = text_api.TextQueryRequest(
            input_text=utterance,
            interaction_history=interaction_history,
            user_id='48dd40a0-f0ba-3ed7-8574-ca4bd906cb7f',
            device_id='9b21ac2d-529f-3724-b154-a31b79058a15',
            context=text_context.Context(
                navigation_context=text_context.NavigationContext(
                    current_location=latlng_pb2.LatLng(
                        latitude=40.754517,
                        longitude=-73.984738
                    ),
                    current_location_source='LOCATION_SOURCE_GPS'
                ),
                vehicle_static_context=text_context.VehicleStaticContext(
                    vehicle_model_code=wrappers_pb2.StringValue(value='P155-G')
                ),
            )
        )
        if session_data:
            req.session_data = session_data

        grpc_results = []
        try:
            response = self.client.TextQuery(req)
            for r in response:
                grpc_results.append(r)

            # Try to get session ID from header metadata
            initial_metadata = response.initial_metadata()
            session_id = next((value for key, value in initial_metadata if key == 'cerence-session-id'),
                              'unknown-session-id')

        except grpc.RpcError as e:
            logger.error("_invoke_grpc", f"gRPC error: {e}")
            return [], "error-session-id"

        return grpc_results, session_id

    @classmethod
    def _safe_message_to_dict(cls, message):
        try:
            return MessageToDict(message)
        except (TypeError, KeyError):
            result = {}
            for field, value in message.ListFields():
                is_message = field.message_type is not None
                is_repeated = getattr(field, "is_repeated", False)

                if is_message and field.message_type.full_name == "google.protobuf.Any":
                    type_name = value.type_url.split("/")[-1]
                    registered_type = cls.TYPE_REGISTRY.get(type_name)
                    if registered_type:
                        typed_message = registered_type()
                        value.Unpack(typed_message)
                        result[field.name] = {
                            "type_url": value.type_url,
                            "value": MessageToDict(typed_message)
                        }
                    else:
                        result[field.name] = {
                            "type_url": value.type_url,
                            "value_hex": value.value.hex()
                        }
                elif is_repeated:
                    if is_message:
                        result[field.name] = [cls._safe_message_to_dict(v) for v in value]
                    else:
                        result[field.name] = list(value)
                elif is_message:
                    result[field.name] = cls._safe_message_to_dict(value)
                else:
                    result[field.name] = value
            return result

    @classmethod
    def _parse_grpc_response(cls, grpc_rst, session_id):
        grpc_result = {
            "session_id": session_id,
            "responses": []
        }

        final_response = None
        final_type = None
        domain_result_suspended = None

        for grpc_response in grpc_rst:
            response_data = {}

            if grpc_response.HasField("status"):
                response_data["status"] = cls._safe_message_to_dict(grpc_response.status)

            text_result_type = grpc_response.WhichOneof("text_result")
            response_data["text_result_type"] = text_result_type
            if text_result_type:
                response_data[text_result_type] = cls._safe_message_to_dict(
                    getattr(grpc_response, text_result_type)
                )

                if text_result_type == "nlu_result_for_arbitration":
                    domain_result_suspended = response_data[text_result_type].get("domain_result_is_suspended")

                if text_result_type == "domain_result":
                    final_type = "domain_result"
                    final_response = response_data[text_result_type]
                elif text_result_type == "metadata" and final_response is None:
                    final_type = "metadata"
                    final_response = response_data[text_result_type]

            grpc_result["responses"].append(response_data)

        # For backward compatibility, also extract the first concept_tree.
        # Keep the old output contract so existing callers can still use grpc_result["concept_tree"].
        if grpc_result["responses"]:
            for response in grpc_result["responses"]:
                domain_result = response.get("domain_result", {})
                interpretations = domain_result.get("interpretations", {})
                contextualized = interpretations.get("contextualized", [])
                if contextualized:
                    concept_tree = contextualized[0].get("conceptTree") or contextualized[0].get("concept_tree")
                    if concept_tree:
                        grpc_result["concept_tree"] = concept_tree
                        break

        grpc_result["final_text_result_type"] = final_type
        grpc_result["final_response"] = final_response
        if domain_result_suspended is not None:
            grpc_result["domain_result_is_suspended"] = domain_result_suspended

        return grpc_result

    def query(self, utterance: str, interaction_history, session_data=None):
        grpc_rst, session_id = self._invoke_grpc(utterance, interaction_history, session_data)
        if not grpc_rst:
            logger.error("request", "gRPC result is empty")
            return {}
        result = self._parse_grpc_response(grpc_rst, session_id)

        final_text_result_type = result.get("final_text_result_type")
        domain_result_is_suspended = result.get("domain_result_is_suspended")
        domain_result_type_url = None
        if final_text_result_type == "domain_result":
            final_response = result.get("final_response") or {}
            domain_result_type_url = (final_response.get("result") or {}).get("type_url")

        logger.info(
            f"final_text_result_type={final_text_result_type}, "
            f"domain_result_is_suspended={domain_result_is_suspended}, "
            f"domain_result_type_url={domain_result_type_url}"
        )

        return result.get("final_response") or result


class GrpcService:

    def __init__(self):
        self.grpc_client = GrpcClient(**grpc_config)

    def query(self, utterance, interaction_history, session_data=None):
        grpc_results = self.grpc_client.query(utterance, interaction_history, session_data)
        return grpc_results
        logger.trace("query", f"{grpc_results}")



def main():
    grpc_service = GrpcService()
    interaction_history = []
    session_data = None
    results = grpc_service.query("nice to have", interaction_history, session_data)
    session_data = base64.b64decode(results["sessionData"]) if results.get("sessionData") else None
    interaction_history = [
        ParseDict(ih, interaction_history_pb2.InteractionHistory())
        for ih in results.get("interactionHistory", [])
    ]
    grpc_results = grpc_service.query("how about tomorrow?", interaction_history, session_data)
    results = json.dumps(grpc_results, indent=4)
    print(results)



if __name__ == '__main__':
    main()