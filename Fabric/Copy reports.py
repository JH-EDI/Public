#!/usr/bin/env python
# coding: utf-8

# ## Copy reports
# 
# Copies report items from a sourceDomains list into tergetWorkspace using rest APIs.
# It handles both existing and non existing copies of the report in the target.
# It cannot perform an intelligent delta copy due to API limitations.

# # Parameters

# In[1]:

targetWorkspace = "<Target Workspace Name>" # This will be replaced by the target workspace name
sourceDomains = [
    "<Source Domain 1>",
    "<Source Domain 2>",
    "<Source Domain 3>"
    # Add more domains as needed
]
tag = "<Tag Name>" # This will be replaced by the tag name to filter reports

# # Libraries

# In[2]:
import requests
import re
import time
import json
from typing import Any, Dict, List, Optional, Union 
# Handle authentication
import msal
# Access to notebookutils for retrieving credentials
from notebookutils import credentials 

# # Base class
# Encapsulates token management, URL building and API calling.

# In[4]:

class BaseApi:
    # Initialize with base_url and scopes.
    def __init__(self, base_url: str, scopes: List[str]):
        self.kv_uri = "https://<yourKeyVault>.vault.azure.net/"  # Replace with your Key Vault URI
        # Retrieve credentials from your secure store using your own secret names.
        self.tenant_id = credentials.getSecret(self.kv_uri, "Fabric-Tenant-ID")
        self.client_id = credentials.getSecret(self.kv_uri, "Fabric-Client-ID")
        self.client_secret = credentials.getSecret(self.kv_uri, "Fabric-Client-Secret")
        
        # Configure the parameters used by derived classes
        self.base_url = base_url
        self.scopes = scopes
        
        # Get the initial access token using the client credentials flow.
        self._refresh_token()
    
    # Default token acquisition using MSAL (client credentials flow).
    def _get_token(self) -> str:
        authority_url = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority_url,
            client_credential=self.client_secret
        )
        result = app.acquire_token_silent(self.scopes, account=None)
        if not result:
            # No token in cache. Acquiring new token from Azure AD.
            result = app.acquire_token_for_client(scopes=self.scopes)
        if "access_token" not in result:
            # JSON usage: Error reporting (internal)
            raise RuntimeError("Could not obtain access token: " + json.dumps(result))
        return result["access_token"]
    
    # Refresh the access token and update headers.
    def _refresh_token(self) -> None:
        # print("Refreshing API token.")
        self.access_token = self._get_token()
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
    
    # Join base URL and endpoint.
    @staticmethod
    def join_url(base_url: str, endpoint: str) -> str:
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
        return f"{base_url}/{endpoint}"

    # Is this string a valid GUID
    @staticmethod
    def is_guid(value: str) -> bool:
        if not isinstance(value, str): # Ensure value is a string before matching
            return False
        guid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        return bool(guid_pattern.match(value)) # Added bool() for clarity
    
    # Return the first key in the provided JSON response that holds a list.
    # Default "value" is common in OData/Fabric APIs, but not guaranteed.
    def get_pagination_array_name(self, content: Dict[str, Any], default_name: str = "value") -> Optional[str]:
        if default_name in content and isinstance(content[default_name], list):
            return default_name
        for k, v in content.items():
            if isinstance(v, list):
                return k
        return None

    # Generic API call handling requests (GET, POST, etc.) with support for pagination,
    # token refresh on 401, throttling (429), and basic LRO (202) logic.
    def api_call(self, uri: str, method: str = "GET", body: Optional[Union[Dict, str]] = None, query_params: Optional[Dict[str, str]] = None) -> List[Any]:
        url = self.join_url(self.base_url, uri)
        all_items: List[Any] = []
        array_name: Optional[str] = None
        first_page = True
        retry_count = 0

        while url:
            # Handles both JSON (dict) and raw string payloads for requests.
            resp = requests.request(method, url, headers=self.headers, json=body if isinstance(body, dict) else None, data=body if isinstance(body, str) else None, params=query_params)
            
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                print(f"Throttling detected (429) for URL: {url}. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            if resp.status_code == 401 and retry_count < 3:
                # print("Unauthorized (401) detected. Refreshing access token and retrying...")
                self._refresh_token()
                retry_count += 1
                continue
            if resp.status_code == 202: # Accepted (Long Running Operation)
                location_url = resp.headers.get("Location")
                status_url = resp.headers.get("Azure-AsyncOperation") or location_url
                print(f"Received HTTP 202 for URL: {url}. Polling status at {status_url if status_url else 'original URL'}...")
                
                # Basic LRO polling: Wait and retry the original request or poll if a status URL is given
                poll_interval = int(resp.headers.get("Retry-After", 10)) 
                time.sleep(poll_interval)
                # If a different status URL is provided, switch to it for GET
                if status_url and status_url != url : 
                    url = status_url 
                    method = "GET" # LRO status checks are typically GET
                    body = None
                    query_params = None
                # If no specific status URL, or it's the same, we just continue the loop (retrying the original op or assuming it's completed)
                continue # Retry the (potentially new) URL

            resp.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            
            # Handle empty response body (e.g., for 204 No Content)
            if not resp.content:
                if method.upper() != "GET" and resp.status_code >= 200 and resp.status_code < 300:
                    # For non-GET successful calls with no content (e.g., DELETE, some POSTs)
                    # Return a conventional success indicator if needed, or just break
                    all_items.append({"status": "success", "statusCode": resp.status_code, "message": "Operation successful with no content."})
                break 

            # JSON usage: Parsing API response
            try:
                content = resp.json()
            except json.JSONDecodeError:
                print(f"Warning: Could not decode JSON from response for URL {url}. Content: {resp.text[:200]}") # Log part of the content
                # Decide how to handle non-JSON response, e.g. if text is expected for some calls.
                # For now, if it's not JSON, we can't process it further in the generic way.
                all_items.append({"raw_response": resp.text}) # Or handle error appropriately
                break

            if first_page:
                # Try to find the array name using "value" as default, or first list encountered
                array_name = self.get_pagination_array_name(content) 
                first_page = False
            
            if array_name and isinstance(content.get(array_name), list):
                all_items.extend(content.get(array_name))
            else: # If no specific array_name found, or content isn't structured with it
                if isinstance(content, list): # Response is directly a list
                    all_items.extend(content)
                elif isinstance(content, dict): # Response is a single object
                    all_items.append(content)
                # else: content is some other type, not processed.

            # Pagination for Fabric/OData style APIs
            next_link = content.get("continuationUri") or content.get("@odata.nextLink")
            
            if next_link:
                url = next_link
                # Body and query_params are usually not needed for "nextLink" GET requests
                body = None 
                query_params = None
            else:
                break 
        return all_items


# # Fabric sub-class
# Uses Fabric specific base url and scopes

# In[5]:


class FabricAPI(BaseApi):
    # Initialize with Fabric-specific URL, scopes, and default output format.
    def __init__(self):
        base_url = "https://api.fabric.microsoft.com/v1"
        scopes = ["https://api.fabric.microsoft.com/.default"]
        super().__init__(base_url, scopes)
    
    def get_all_domains(self) -> List[Dict[str, Any]]:
        return self.api_call("admin/domains")

    def get_subdomains(self, domain: str) -> List[Dict[str, Any]]:
        all_domains = self.get_all_domains()
        parent = None
        for d in all_domains:
            if d.get("id") == domain or d.get("displayName") == domain:
                parent = d
                break
        if not parent:
            raise ValueError(f"No domain found with identifier: {domain}")
        parent_id = parent.get("id")
        subdomains = [d for d in all_domains if d.get("parentId") == parent_id]
        return [parent] + subdomains

    def get_domain_workspaces(self, domain_id: str) -> List[Dict[str, Any]]:
        return self.api_call(f"admin/domains/{domain_id}/workspaces")

    def get_all_subdomain_workspaces(self, domain: str) -> List[Dict[str, Any]]:
        domains = self.get_subdomains(domain)
        all_workspaces = []
        for d in domains:
            ws = self.get_domain_workspaces(d["id"])
            all_workspaces.extend(ws)
        return all_workspaces

    def resolve_workspace_id(self, workspace_name: str) -> str:
        workspaces = self.api_call("admin/workspaces", query_params={"name": workspace_name})
        if workspaces and len(workspaces) == 1:
            return workspaces[0].get("id")
        raise ValueError(f"No workspace found with name: {workspace_name}")

    def get_reports(self, workspace: str, tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not self.is_guid(workspace):
            workspace = self.resolve_workspace_id(workspace)
        reports = self.api_call(f"workspaces/{workspace}/reports")
        if tags:
            reports = [
                r for r in reports
                if any(
                    candidate == tag.get("displayName") or candidate == tag.get("id")
                    for candidate in tags
                    for tag in r.get("tags", [])
                )
            ]
        return reports


# # Power BI API subclass
# Subclass using Power BI–specific base URL and scopes.

# In[6]:


class PowerBiAPI(BaseApi):
    # Initialize with Power BI default URL, scopes, and an output format.
    def __init__(self):
        base_url = "https://api.powerbi.com/v1.0/myorg"
        scopes = ["https://analysis.windows.net/powerbi/api/.default"]
        super().__init__(base_url, scopes)
    
    def resolve_workspace_id(self, workspace_name: str) -> str:
        query_params = {"$filter": f"name eq '{workspace_name}'"}
        workspaces = self.api_call("groups", query_params=query_params)
        if workspaces and "value" in workspaces and len(workspaces["value"]) == 1:
            return workspaces["value"][0]["id"]
        raise ValueError(f"No workspace found with name: {workspace_name}")

    def get_reports(self, workspace: str, tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if not self.is_guid(workspace):
            workspace = self.resolve_workspace_id(workspace)
        reports = self.api_call(f"groups/{workspace}/reports")
        if tags:
            reports = [
                r for r in reports
                if any(
                    candidate == tag.get("displayName") or candidate == tag.get("id")
                    for candidate in tags
                    for tag in r.get("tags", [])
                )
            ]
        return reports

    # Copy or update a report in the target workspace depending on existence.
    def refresh_report(self, report_id: str, source_workspace_id: str, target_workspace_id: str, new_report_name: str) -> Dict[str, Any]:
        existing_reports = self.get_reports(target_workspace_id)
        existing_report = next((r for r in existing_reports if r.get("name") == new_report_name), None)
        if existing_report:
            target_report_id = existing_report["id"]
            endpoint = f"groups/{target_workspace_id}/reports/{target_report_id}/UpdateReportContent"
            payload = {
                "sourceReport": {
                    "sourceReportId": report_id,
                    "sourceWorkspaceId": source_workspace_id
                },
                "sourceType": "ExistingReport"
            }
            return self.api_call(endpoint, method="POST", body=payload)
        else:
            endpoint = f"groups/{source_workspace_id}/reports/{report_id}/Clone"
            payload = {
                "name": new_report_name,
                "targetWorkspaceId": target_workspace_id
            }
            return self.api_call(endpoint, method="POST", body=payload)


# # Driver
# This script drives the execution of the report copy/update process.

# In[7]:


fab = FabricAPI()
pbi = PowerBiAPI()
targetWorkspaceId = fab.resolve_workspace_id(targetWorkspace)
print(f"Target workspace = {targetWorkspace}({targetWorkspaceId})")

all_workspaces = []
for domain in sourceDomains:
    domain_workspaces = fab.get_all_subdomain_workspaces(domain)
    print(f"Found {len(domain_workspaces)} workspaces in domain/subdomains '{domain}'.")
    all_workspaces.extend(domain_workspaces)

for ws in all_workspaces:
    ws_id = ws["id"]
    ws_name = ws.get("displayName", "")
    try:
        reports = fab.get_reports(ws_id, tags=[tag])
    except Exception as e:
        print(f"Skipping workspace '{ws_name}' ({ws_id}) due to error: {e}")
        continue
    if not reports:
        continue
    print(f"Workspace '{ws_name}' ({ws_id}) has {len(reports)} tagged reports.")
    for report in reports:
        report_id = report["id"]
        report_name = report["displayName"]
        try:
            pbi.refresh_report(
                report_id=report_id,
                source_workspace_id=ws_id,
                target_workspace_id=targetWorkspaceId,
                new_report_name=report_name
            )
            print(f"Refreshed report {report_name} from workspace '{ws_name}' to {targetWorkspace}")
        except Exception as e:
            print(f"Failed to refresh/copy report '{report_name}' from workspace '{ws_name}': {e}")

