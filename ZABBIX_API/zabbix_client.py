import requests
import urllib3

# Suprimir warnings de HTTPS não verificados (usar com cuidado)
try:
	urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
	pass
import json
from typing import List, Dict, Any, Optional


class ZabbixClient:
	"""Cliente básico para API Zabbix via JSON-RPC.

	Usa token de API (Bearer-style) colocado no campo `auth` das requisições.
	Métodos implementados: `get_events` e `get_hosts`.
	"""

	def __init__(self, base_url: str = None, api_token: str = None, verify: Optional[bool] = False):
		self.base_url = base_url.rstrip('/') if base_url else base_url
		self.api_token = api_token
		self.verify = verify
		self.session = requests.Session()
		# configure urllib3 Retry on the session to handle transient connection issues
		try:
			from requests.adapters import HTTPAdapter
			from urllib3.util.retry import Retry
			retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504], allowed_methods=["POST", "GET"])
			adapter = HTTPAdapter(max_retries=retry_strategy)
			self.session.mount("https://", adapter)
			self.session.mount("http://", adapter)
		except Exception:
			pass

	def _call(self, method: str, params: Dict[str, Any]) -> Any:
		payload = {
			"jsonrpc": "2.0",
			"method": method,
			"params": params,
			"auth": self.api_token,
			"id": 1,
		}
		headers = {"Content-Type": "application/json-rpc"}
		try:
			resp = self.session.post(self.base_url, headers=headers, data=json.dumps(payload), timeout=30, verify=self.verify)
			resp.raise_for_status()
			try:
				data = resp.json()
			except json.JSONDecodeError as json_err:
				print(f"[ZabbixClient ERROR] Invalid JSON response for {method}: status={resp.status_code}, text='{resp.text[:200]}...'")
				return []
			if 'error' in data:
				raise Exception(data['error'])
			return data.get('result', [])
		except requests.exceptions.SSLError as ssl_err:
			# Retry once without verification if SSL verification fails
			try:
				resp = self.session.post(self.base_url, headers=headers, data=json.dumps(payload), timeout=30, verify=False)
				resp.raise_for_status()
				try:
					data = resp.json()
				except json.JSONDecodeError as json_err:
					print(f"[ZabbixClient ERROR] Invalid JSON response after SSL fallback for {method}: status={resp.status_code}, text='{resp.text[:200]}...'")
					return []
				if 'error' in data:
					raise Exception(data['error'])
				return data.get('result', [])
			except Exception as e:
				print(f"[ZabbixClient ERROR] call {method} failed after SSL fallback: {e}")
				return []
		except Exception as e:
			# Generic connection errors: retry a few times with backoff before giving up
			from requests.exceptions import RequestException
			from time import sleep
			retries = 3
			for attempt in range(retries):
				try:
					resp = self.session.post(self.base_url, headers=headers, data=json.dumps(payload), timeout=30, verify=self.verify)
					resp.raise_for_status()
					try:
						data = resp.json()
					except json.JSONDecodeError as json_err:
						if attempt == retries - 1:
							print(f"[ZabbixClient ERROR] Invalid JSON response for {method} after retries: status={resp.status_code}, text='{resp.text[:200]}...'")
							return []
						sleep(2 ** attempt)
						continue
					if 'error' in data:
						if attempt == retries - 1:
							print(f"[ZabbixClient ERROR] API error for {method} after retries: {data['error']}")
							return []
						sleep(2 ** attempt)
						continue
					return data.get('result', [])
				except Exception as inner_e:
					if attempt == retries - 1:
						print(f"[ZabbixClient ERROR] call {method} failed: {inner_e}")
						return []
					# small exponential backoff
					sleep(2 ** attempt)

	def get_events(self, time_from: Optional[int] = None, time_till: Optional[int] = None, objectids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
		"""Recupera eventos de problema (source=0, object=0, value=1) no período informado."""
		params = {
			"output": ["eventid", "objectid", "clock", "name", "value", "severity", "r_eventid", "acknowledged", "ns"],
			"source": 0,
			"object": 0,
			"value": 1,
			"selectHosts": ["hostid", "name"],
			"sortfield": "clock",
			"sortorder": "ASC",
			"limit": 10000,
		}
		# If caller provided trigger/object ids to filter, include them
		if objectids:
			# Zabbix expects a list or comma-separated string; pass list when possible
			params['objectids'] = objectids
		if time_from is not None:
			params['time_from'] = int(time_from)
		if time_till is not None:
			params['time_till'] = int(time_till)

		return self._call('event.get', params)

	def get_events_paginated(self, time_from: Optional[int] = None, time_till: Optional[int] = None, objectids: Optional[List[str]] = None, page_size: int = 10000) -> List[Dict[str, Any]]:
		"""Recupera eventos em páginas (limit + limit_from) até exaurir resultados.

		Retorna lista concatenada de todos os eventos obtidos.
		"""
		all_results: List[Dict[str, Any]] = []
		offset = 0
		while True:
			params = {
				"output": ["eventid", "objectid", "clock", "name", "value", "severity", "r_eventid", "acknowledged", "ns"],
				"source": 0,
				"object": 0,
				"value": 1,
				"selectHosts": ["hostid", "name"],
				"sortfield": "clock",
				"sortorder": "ASC",
				"limit": int(page_size),
				"limit_from": int(offset),
			}
			if objectids:
				params['objectids'] = objectids
			if time_from is not None:
				params['time_from'] = int(time_from)
			if time_till is not None:
				params['time_till'] = int(time_till)

			page = self._call('event.get', params)
			if not page:
				break
			all_results.extend(page)
			if len(page) < int(page_size):
				break
			offset += int(page_size)
		# dedupe by eventid just in case
		seen = set()
		unique = []
		for ev in all_results:
			eid = ev.get('eventid')
			if eid and eid not in seen:
				seen.add(eid)
				unique.append(ev)
		return unique

	def get_hosts(self, *args, **kwargs) -> List[Dict[str, Any]]:
		params = {
			"output": ["hostid", "host", "name", "status"],
			"selectInterfaces": ["ip"],
			"limit": 10000,
		}
		return self._call('host.get', params)

	def get_triggers(self, status: Optional[int] = 0, with_hosts: bool = True, limit: int = 10000) -> List[Dict[str, Any]]:
		"""Recupera triggers (por padrão apenas ativas status=0)."""
		params = {
			"output": ["triggerid", "description", "priority", "status", "lastchange"],
			"filter": {"status": str(status)},
			"sortfield": "description",
			"sortorder": "ASC",
			"limit": limit,
		}
		if with_hosts:
			params['selectHosts'] = ["hostid", "name"]
		return self._call('trigger.get', params)

	def get_maintenances(self, limit: int = 10000) -> List[Dict[str, Any]]:
		"""Recupera manutenções."""
		params = {
			"output": "extend",
			"selectHostGroups": "extend",
			"selectHosts": "extend",
			"selectTimeperiods": "extend",
			"selectTags": "extend",
			"sortfield": "name",
			"sortorder": "ASC",
			"limit": limit,
		}
		return self._call('maintenance.get', params)

	def get_event_by_id(self, eventid: str, value: Optional[int] = None, time_from: Optional[int] = None) -> List[Dict[str, Any]]:
		"""Recupera um evento específico pelo eventid, com opção de filtrar por `value` e `time_from`."""
		params = {
			"output": ["eventid", "objectid", "clock", "name", "value", "severity", "r_eventid", "acknowledged", "ns"],
			"eventids": str(eventid),
			"selectHosts": ["hostid", "name"],
			"limit": 1,
		}
		if value is not None:
			params['value'] = int(value)
		if time_from is not None:
			params['time_from'] = int(time_from)

		return self._call('event.get', params)

	def get_events_by_ids(self, eventids: List[str], value: Optional[int] = None, time_from: Optional[int] = None) -> List[Dict[str, Any]]:
		"""Recupera múltiplos eventos por `eventids` em uma única chamada (útil para resoluções)."""
		if not eventids:
			return []
		params = {
			"output": ["eventid", "objectid", "clock", "name", "value", "severity", "r_eventid", "acknowledged", "ns"],
			"eventids": eventids,
			"selectHosts": ["hostid", "name"],
			"limit": len(eventids) or 1,
		}
		if value is not None:
			params['value'] = int(value)
		if time_from is not None:
			params['time_from'] = int(time_from)

		return self._call('event.get', params)

	def get_acknowledges_for_events(self, eventids: List[str]) -> List[Dict[str, Any]]:
		"""Recupera acknowledges para uma lista de eventos (select_acknowledges=extend)."""
		if not eventids:
			return []
		params = {
			"output": ["eventid"],
			"eventids": eventids,
			"select_acknowledges": "extend",
			"limit": len(eventids) or 1,
		}
		return self._call('event.get', params)

	def get_users(self, userids: List[str]) -> List[Dict[str, Any]]:
		"""Recupera informações de usuários por `userids`."""
		if not userids:
			return []
		params = {
			"output": ["userid", "username", "name", "surname"],
			"userids": userids,
			"limit": len(userids) or 1,
		}
		return self._call('user.get', params)

