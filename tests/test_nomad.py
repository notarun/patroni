import json
import os
import socket
import socketserver
import tempfile
import threading
import unittest

from http.server import BaseHTTPRequestHandler
from typing import Any, cast, Dict, Mapping, Optional
from unittest.mock import Mock

import requests
import requests_unixsocket

from patroni.dcs import Cluster, Leader, Member
from patroni.dcs.nomad import Nomad, NomadClient, NomadConflict, NomadError, NomadNotFound
from patroni.postgresql.mpp import AbstractMPP, get_mpp


class UnixSocketHandler(BaseHTTPRequestHandler):

    def do_GET(self) -> None:
        body = json.dumps({'Path': self.path[len('/v1/var/'):]}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


def variable(path: str, value: str = '', index: int = 1, lock_id: Optional[str] = None) -> Dict[str, Any]:
    ret: Dict[str, Any] = {'Path': path, 'ModifyIndex': index, 'Items': {'value': value}}
    if lock_id:
        ret['Lock'] = {'ID': lock_id, 'TTL': '30s', 'LockDelay': '10s'}
    return ret


class TestNomadClient(unittest.TestCase):

    def setUp(self) -> None:
        self.client = NomadClient(token='secret', namespace='testing')
        self.request = Mock()
        setattr(self.client.session, 'request', self.request)

    @staticmethod
    def response(status: int = 200, data: bytes = b'{}',
                 headers: Optional[Mapping[str, str]] = None) -> requests.Response:
        response = requests.Response()
        response.status_code = status
        setattr(response, '_content', data)
        response.headers.update(headers or {})
        return response

    def test_request(self):
        self.request.return_value = self.response(
            data=b'{"ModifyIndex":2}', headers={'X-Nomad-Index': '3'})
        ret = self.client.put_variable('service/a b/config', '{}', cas=1)

        self.assertEqual(ret['ModifyIndex'], 2)
        request = self.request.call_args
        assert request is not None
        args, kwargs = request
        self.assertEqual(args[0], 'PUT')
        self.assertIn('/v1/var/service/a%20b/config', args[1])
        self.assertEqual(kwargs['params'], {'cas': 1, 'namespace': 'testing'})
        self.assertEqual(self.client.session.headers['X-Nomad-Token'], 'secret')
        self.assertEqual(kwargs['json'], {'Items': {'value': '{}'}})
        self.assertEqual(kwargs['timeout'], 10)
        self.assertFalse(kwargs['allow_redirects'])
        self.assertFalse(self.client.session.trust_env)

    def test_statuses(self):
        self.request.return_value = self.response(404, b'not found')
        self.assertRaises(NomadNotFound, self.client.get_variable, 'missing')
        self.request.return_value = self.response(409, b'conflict')
        self.assertRaises(NomadConflict, self.client.put_variable, 'key', 'value', 1)
        self.request.return_value = self.response(500, b'broken')
        self.assertRaises(NomadError, self.client.get_variable, 'key')
        self.request.return_value = self.response(302, b'redirect')
        self.assertRaises(NomadError, self.client.get_variable, 'key')
        self.request.return_value = self.response(200, b'{')
        self.assertRaises(NomadError, self.client.get_variable, 'key')

    def test_lock_requests(self):
        self.request.return_value = self.response(data=b'{"Lock":{"ID":"123"}}')
        self.assertEqual(self.client.acquire_lock('leader', 'node1', 30)['Lock']['ID'], '123')
        request = self.request.call_args
        assert request is not None
        self.assertEqual(request.kwargs['params']['lock-acquire'], '')
        self.assertEqual(request.kwargs['json']['Lock'], {'TTL': '30s', 'LockDelay': '10s'})

        self.client.renew_lock('leader', '123')
        request = self.request.call_args
        assert request is not None
        self.assertIn('lock-renew', request.kwargs['params'])
        self.client.release_lock('leader', '123')
        request = self.request.call_args
        assert request is not None
        body = request.kwargs['json']
        self.assertIn('lock-release', request.kwargs['params'])
        self.assertNotIn('Items', body)

        self.client.acquire_lock('leader', 'node1', 30, '123')
        request = self.request.call_args
        assert request is not None
        self.assertEqual(request.kwargs['json']['Lock']['ID'], '123')

    def test_list_pagination_and_delete(self):
        self.request.side_effect = [
            self.response(data=b'[{"Path":"service/a"}]', headers={'X-Nomad-NextToken': 'next'}),
            self.response(data=b'[{"Path":"service/b"}]'),
            self.response(status=204, data=b'')]
        self.assertEqual([v['Path'] for v in self.client.list_variables('service/')], ['service/a', 'service/b'])
        self.assertEqual(self.request.call_args_list[1].kwargs['params']['next_token'], 'next')
        self.assertTrue(self.client.delete_variable('service/a', 1))

    @unittest.skipUnless(hasattr(socket, 'AF_UNIX') and hasattr(socketserver, 'UnixStreamServer'),
                         'Unix sockets are not supported')
    def test_unix_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'api.sock')
            server = socketserver.UnixStreamServer(path, UnixSocketHandler)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                client = NomadClient(unix_socket=path)
                self.assertIsInstance(client.session, requests_unixsocket.Session)
                self.assertEqual(client.get_variable('service/test/config')['Path'], 'service/test/config')
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


class NomadForTests(Nomad):

    @property
    def name(self) -> str:
        return self._name

    @property
    def member_lock(self) -> Optional[str]:
        return self._member_lock

    @property
    def leader_lock(self) -> Optional[str]:
        return self._leader_lock

    @property
    def nomad_client(self) -> NomadClient:
        return self._client

    def set_client(self, client: NomadClient) -> None:
        self._client = client

    def set_cached_cluster(self, cluster: Cluster) -> None:
        self._cluster = cluster
        self._cluster_valid_till = float('inf')

    def set_mpp(self, mpp: AbstractMPP) -> None:
        self._mpp = mpp

    def set_leader_lock(self, lock_id: str) -> None:
        self._leader_lock = lock_id

    def update_leader_lock(self, leader: Leader) -> bool:
        return self._update_leader(leader)

    def delete_leader_lock(self, leader: Leader) -> bool:
        return self._delete_leader(leader)

    def write_status_value(self, value: str) -> bool:
        return self._write_status(value)

    def write_failsafe_value(self, value: str) -> bool:
        return self._write_failsafe(value)

    def write_leader_optime_value(self, value: str) -> bool:
        return self._write_leader_optime(value)


class TestNomad(unittest.TestCase):

    def setUp(self) -> None:
        self.c = NomadForTests({'scope': 'test', 'name': 'postgresql1', 'ttl': 30, 'retry_timeout': 10,
                                'loop_wait': 10, 'host': 'localhost:4646'}, get_mpp({}))
        self.client = Mock(spec=NomadClient)
        self.c.set_client(cast(NomadClient, self.client))

    def load_fixture(self, unlocked: bool = False) -> None:
        prefix = 'service/test/'
        values = {
            prefix + 'initialize': variable(prefix + 'initialize', 'sysid', 1),
            prefix + 'config': variable(prefix + 'config', '{"ttl":30}', 2),
            prefix + 'history': variable(prefix + 'history', '[[1,2,"x"]]', 3),
            prefix + 'status': variable(prefix + 'status', '{"optime":42,"slots":{"a":1}}', 4),
            prefix + 'members/postgresql1': variable(prefix + 'members/postgresql1',
                                                     '{"conn_url":"postgres://localhost/postgres"}', 5,
                                                     None if unlocked else 'member-lock'),
            prefix + 'members/stale': variable(prefix + 'members/stale', '{}', 6),
            prefix + 'leader': variable(prefix + 'leader', 'postgresql1', 7,
                                        None if unlocked else 'leader-lock'),
            prefix + 'failover': variable(prefix + 'failover', '{"leader":"postgresql0"}', 8),
            prefix + 'sync': variable(prefix + 'sync', '{"leader":"postgresql1","sync_standby":"postgresql0"}', 9),
            prefix + 'failsafe': variable(prefix + 'failsafe', '{"postgresql1":"http://localhost:8008"}', 10)}
        self.client.list_variables.return_value = [{'Path': path} for path in values]

        def get_variable(path: str, deadline: Optional[float] = None) -> Dict[str, Any]:
            return values[path]

        self.client.get_variable.side_effect = get_variable

    def test_get_cluster(self):
        self.load_fixture()
        cluster = self.c.get_cluster()
        self.assertIsInstance(cluster, Cluster)
        self.assertEqual(cluster.initialize, 'sysid')
        assert cluster.leader is not None
        self.assertEqual(cluster.leader.name, 'postgresql1')
        self.assertEqual(cluster.leader.session, 'leader-lock')
        self.assertEqual([m.name for m in cluster.members], ['postgresql1'])
        self.assertEqual(cluster.status.last_lsn, 42)
        self.assertEqual(cluster.sync.leader, 'postgresql1')
        self.assertEqual(cluster.failsafe, {'postgresql1': 'http://localhost:8008'})

    def test_unlocked_records_are_stale(self):
        self.load_fixture(unlocked=True)
        cluster = self.c.get_cluster()
        self.assertIsNone(cluster.leader)
        self.assertEqual(cluster.members, [])

    def test_empty_and_disappearing_variables(self):
        self.client.list_variables.return_value = []
        self.assertIsNone(self.c.get_cluster().leader)
        self.client.list_variables.return_value = [{'Path': 'service/test/config'}]
        self.client.get_variable.side_effect = NomadNotFound('gone')
        self.assertIsNone(self.c.get_cluster().config)
        self.client.list_variables.side_effect = NomadError('down')
        self.assertRaises(NomadError, self.c.get_cluster)

    def test_touch_member(self):
        self.client.acquire_lock.return_value = variable(self.c.member_path, '{}', 1, 'member-lock')
        data = {'conn_url': 'postgres://localhost/postgres'}
        self.assertTrue(self.c.touch_member(data))
        self.assertEqual(self.c.member_lock, 'member-lock')
        self.assertTrue(self.c.touch_member(data))
        self.client.renew_lock.assert_called_with(self.c.member_path, 'member-lock')

        changed = {'conn_url': 'postgres://localhost/postgres', 'role': 'replica'}
        self.assertTrue(self.c.touch_member(changed))
        self.client.acquire_lock.assert_called_with(self.c.member_path,
                                                    json.dumps(changed, separators=(',', ':')),
                                                    30, 'member-lock')

        self.client.renew_lock.side_effect = NomadConflict('lost')
        self.assertFalse(self.c.touch_member(changed))
        self.assertIsNone(self.c.member_lock)

    def test_recovers_member_lock_after_restart(self):
        data = {'conn_url': 'postgres://localhost/postgres'}
        cluster = Cluster(None, None, None, Mock(), [Member(1, self.c.name, 'existing-lock', data)],
                          None, Mock(), None, None)
        self.c.set_cached_cluster(cluster)
        self.assertTrue(self.c.touch_member(data))
        self.client.acquire_lock.assert_not_called()
        self.client.renew_lock.assert_called_once_with(self.c.member_path, 'existing-lock')

    def test_leader_lifecycle(self):
        self.client.acquire_lock.return_value = variable(self.c.leader_path, self.c.name, 1, 'leader-lock')
        self.assertTrue(self.c.attempt_to_acquire_leader())
        self.assertEqual(self.c.leader_lock, 'leader-lock')
        leader = Leader(1, 'leader-lock', Member(-1, self.c.name, None, {}))
        self.assertTrue(self.c.update_leader_lock(leader))
        self.client.renew_lock.assert_called_with(self.c.leader_path, 'leader-lock')

        self.client.release_lock.return_value = {'ModifyIndex': 2}
        self.assertTrue(self.c.delete_leader_lock(leader))
        self.client.delete_variable.assert_called_with(self.c.leader_path, 2)
        self.assertIsNone(self.c.leader_lock)

    def test_recovers_leader_lock_after_restart(self):
        leader = Leader(1, 'existing-lock', Member(-1, self.c.name, None, {}))
        self.assertTrue(self.c.update_leader_lock(leader))
        self.client.renew_lock.assert_called_once_with(self.c.leader_path, 'existing-lock')

    def test_leader_conflicts_and_ownership(self):
        self.client.acquire_lock.side_effect = NomadConflict('held')
        self.assertFalse(self.c.attempt_to_acquire_leader())
        self.client.acquire_lock.side_effect = NomadError('down')
        self.assertRaises(NomadError, self.c.attempt_to_acquire_leader)

        leader = Leader(1, 'other-lock', Member(-1, self.c.name, None, {}))
        self.c.set_leader_lock('leader-lock')
        self.assertFalse(self.c.delete_leader_lock(leader))
        self.client.release_lock.assert_not_called()

    def test_persistent_values(self):
        self.client.put_variable.return_value = {'ModifyIndex': 12}
        self.assertTrue(self.c.set_config_value('{}', 1))
        self.assertTrue(self.c.set_failover_value('{}'))
        self.assertTrue(self.c.initialize(True, 'sysid'))
        self.assertEqual(self.c.set_sync_state_value('{}', 2), 12)
        self.assertTrue(self.c.set_history_value('[]'))
        self.assertTrue(self.c.write_status_value('{}'))
        self.assertTrue(self.c.write_failsafe_value('{}'))
        self.assertTrue(self.c.write_leader_optime_value('1'))

        self.assertTrue(self.c.cancel_initialization())
        self.assertTrue(self.c.delete_sync_state(2))
        self.client.list_variables.return_value = [{'Path': 'service/test/config', 'ModifyIndex': 12}]
        self.client.get_variable.return_value = variable('service/test/config', '{}', 12)
        self.assertTrue(self.c.delete_cluster())

    def test_mpp_cluster(self):
        self.c.set_mpp(get_mpp({'citus': {'group': 0, 'database': 'postgres'}}))
        value = variable('service/test/0/initialize', 'sysid', 1)
        self.client.list_variables.return_value = [{'Path': value['Path']}]
        self.client.get_variable.return_value = value
        cluster = self.c.get_cluster()
        self.assertEqual(cluster.initialize, 'sysid')

    def test_ttl_validation(self):
        self.assertRaises(ValueError, self.c.set_ttl, 9)
        self.assertRaises(ValueError, self.c.set_ttl, 86401)
        self.assertTrue(self.c.set_ttl(31))
        self.assertEqual(self.c.ttl, 31)

    def test_reload_config(self):
        self.c.reload_config({'loop_wait': 5, 'ttl': 30, 'retry_timeout': 6,
                              'nomad': {'url': 'https://nomad.example:4647', 'token': 'new'}})
        self.assertEqual(self.c.nomad_client.base_uri, 'https://nomad.example:4647')
        self.assertEqual(self.c.nomad_client.session.headers['X-Nomad-Token'], 'new')

    def test_unix_socket_config(self):
        for config in ({'host': '/secrets/api.sock'}, {'url': 'unix:///secrets/api.sock'},
                       {'url': 'unix:/secrets/api.sock'}):
            client = NomadClient.from_config(config)
            self.assertEqual(client.base_uri, 'http+unix://%2Fsecrets%2Fapi.sock')
        self.assertRaises(ValueError, NomadClient.from_config,
                          {'host': '/secrets/api.sock', 'verify': False})


if __name__ == '__main__':
    unittest.main()
