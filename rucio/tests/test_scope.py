# -*- coding: utf-8 -*-
# Copyright 2012-2020 CERN for the benefit of the ATLAS collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
# - Thomas Beermann <thomas.beermann@cern.ch>, 2012-2013
# - Angelos Molfetas <Angelos.Molfetas@cern.ch>, 2012
# - Mario Lassnig <mario.lassnig@cern.ch>, 2012
# - Vincent Garonne <vincent.garonne@cern.ch>, 2012-2017
# - Cedric Serfon <cedric.serfon@cern.ch>, 2017
# - Joaquín Bogado <jbogado@linti.unlp.edu.ar>, 2018
# - Andrew Lister <andrew.lister@stfc.ac.uk>, 2019
# - Patrick Austin <patrick.austin@stfc.ac.uk>, 2020
# - Benedikt Ziemons <benedikt.ziemons@cern.ch>, 2020

import unittest
from json import dumps, loads

import pytest
from paste.fixture import TestApp

from rucio.client.accountclient import AccountClient
from rucio.client.scopeclient import ScopeClient
from rucio.common.config import config_get, config_get_bool
from rucio.common.exception import AccountNotFound, Duplicate, ScopeNotFound, InvalidObject
from rucio.common.types import InternalAccount, InternalScope
from rucio.common.utils import generate_uuid as uuid
from rucio.core.scope import get_scopes, add_scope, is_scope_owner
from rucio.tests.common import account_name_generator, scope_name_generator
from rucio.web.rest.account import APP as account_app
from rucio.web.rest.authentication import APP as auth_app


class TestScopeCoreApi(unittest.TestCase):

    def setUp(self):
        if config_get_bool('common', 'multi_vo', raise_exception=False, default=False):
            self.vo = {'vo': config_get('client', 'vo', raise_exception=False, default='tst')}
        else:
            self.vo = {}

        self.scopes = [InternalScope(scope_name_generator(), **self.vo) for _ in range(5)]
        self.jdoe = InternalAccount('jdoe', **self.vo)

    def test_list_scopes(self):
        """ SCOPE (CORE): List scopes """
        for scope in self.scopes:
            add_scope(scope=scope, account=self.jdoe)
        scopes = get_scopes(account=self.jdoe)
        for scope in scopes:
            assert scope in scopes

    def test_is_scope_owner(self):
        """ SCOPE (CORE): Is scope owner """
        scope = InternalScope(scope_name_generator(), **self.vo)
        add_scope(scope=scope, account=self.jdoe)
        anwser = is_scope_owner(scope=scope, account=self.jdoe)
        assert anwser is True


class TestScope(unittest.TestCase):

    def setUp(self):
        if config_get_bool('common', 'multi_vo', raise_exception=False, default=False):
            self.vo_header = {'X-Rucio-VO': config_get('client', 'vo', raise_exception=False, default='tst')}
        else:
            self.vo_header = {}
        self.scopes = [scope_name_generator() for _ in range(5)]

    def test_scope_success(self):
        """ SCOPE (REST): send a POST to create a new account and scope """
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))

        headers2 = {'X-Rucio-Auth-Token': str(token)}
        acntusr = account_name_generator()
        data = dumps({'type': 'USER', 'email': 'rucio.email.com'})
        res2 = TestApp(account_app.wsgifunc(*mw)).post('/' + acntusr, headers=headers2, params=data, expect_errors=True)
        assert res2.status == 201

        headers3 = {'X-Rucio-Auth-Token': str(token)}
        scopeusr = scope_name_generator()
        res3 = TestApp(account_app.wsgifunc(*mw)).post('/%s/scopes/%s' % (acntusr, scopeusr), headers=headers3, expect_errors=True)
        assert res3.status == 201

    def test_scope_failure(self):
        """ SCOPE (REST): send a POST to create a new scope for a not existing account to test the error"""
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))
        headers2 = {'X-Rucio-Auth-Token': str(token)}
        scopeusr = scope_name_generator()
        account_name_generator()
        res2 = TestApp(account_app.wsgifunc(*mw)).post('/%s/scopes/%s' % (scopeusr, scopeusr), headers=headers2, expect_errors=True)
        assert res2.status == 404

    def test_scope_duplicate(self):
        """ SCOPE (REST): send a POST to create a already existing scope to test the error"""
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))

        headers2 = {'X-Rucio-Auth-Token': str(token)}
        acntusr = account_name_generator()
        data = dumps({'type': 'USER', 'email': 'rucio@email.com'})
        res2 = TestApp(account_app.wsgifunc(*mw)).post('/' + acntusr, headers=headers2, params=data, expect_errors=True)
        assert res2.status == 201

        headers3 = {'X-Rucio-Auth-Token': str(token)}
        scopeusr = scope_name_generator()
        res3 = TestApp(account_app.wsgifunc(*mw)).post('/%s/scopes/%s' % (acntusr, scopeusr), headers=headers3, expect_errors=True)
        assert res3.status == 201
        res3 = TestApp(account_app.wsgifunc(*mw)).post('/%s/scopes/%s' % (acntusr, scopeusr), headers=headers3, expect_errors=True)
        assert res3.status == 409

    def test_list_scope(self):
        """ SCOPE (REST): send a GET list all scopes for one account """
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))

        tmp_val = account_name_generator()
        headers2 = {'Rucio-Type': 'user', 'X-Rucio-Account': 'root', 'X-Rucio-Auth-Token': str(token)}
        data = dumps({'type': 'USER', 'email': 'rucio@email.com'})
        res2 = TestApp(account_app.wsgifunc(*mw)).post('/%s' % tmp_val, headers=headers2, params=data, expect_errors=True)
        assert res2.status == 201

        headers3 = {'X-Rucio-Auth-Token': str(token)}

        for scope in self.scopes:
            data = dumps({})
            res3 = TestApp(account_app.wsgifunc(*mw)).post('/%s/scopes/%s' % (tmp_val, scope), headers=headers3, params=data, expect_errors=True)
            assert res3.status == 201

        res4 = TestApp(account_app.wsgifunc(*mw)).get('/%s/scopes/' % tmp_val, headers=headers3, expect_errors=True)

        assert res4.status == 200

        svr_list = loads(res4.body)
        for scope in self.scopes:
            assert scope in svr_list

    def test_list_scope_account_not_found(self):
        """ SCOPE (REST): send a GET list all scopes for a not existing account """
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))

        headers3 = {'X-Rucio-Auth-Token': str(token)}
        res3 = TestApp(account_app.wsgifunc(*mw)).get('/testaccount/scopes', headers=headers3, expect_errors=True)

        assert res3.status == 404
        assert res3.header('ExceptionClass') == 'AccountNotFound'

    def test_list_scope_no_scopes(self):
        """ SCOPE (REST): send a GET list all scopes for one account without scopes """
        mw = []

        headers1 = {'X-Rucio-Account': 'root', 'X-Rucio-Username': 'ddmlab', 'X-Rucio-Password': 'secret'}
        headers1.update(self.vo_header)
        res1 = TestApp(auth_app.wsgifunc(*mw)).get('/userpass', headers=headers1, expect_errors=True)
        assert res1.status == 200

        token = str(res1.header('X-Rucio-Auth-Token'))

        headers2 = {'X-Rucio-Auth-Token': str(token)}
        acntusr = account_name_generator()
        data = dumps({'type': 'USER', 'email': 'rucio@email.com'})
        res2 = TestApp(account_app.wsgifunc(*mw)).post('/' + acntusr, headers=headers2, params=data, expect_errors=True)
        assert res2.status == 201

        headers3 = {'X-Rucio-Auth-Token': str(token)}

        res4 = TestApp(account_app.wsgifunc(*mw)).get('/%s/scopes/' % (acntusr), headers=headers3, params=data, expect_errors=True)

        assert res4.status == 404
        assert res4.header('ExceptionClass') == 'ScopeNotFound'


class TestScopeClient(unittest.TestCase):

    def setUp(self):
        self.account_client = AccountClient()
        self.scope_client = ScopeClient()

    def test_create_scope(self):
        """ SCOPE (CLIENTS): create a new scope."""
        account = 'jdoe'
        scope = scope_name_generator()
        ret = self.scope_client.add_scope(account, scope)
        assert ret
        with pytest.raises(InvalidObject):
            self.scope_client.add_scope(account, 'tooooolooooongscooooooooooooope')
        with pytest.raises(InvalidObject):
            self.scope_client.add_scope(account, '$?!')

    def test_create_scope_no_account(self):
        """ SCOPE (CLIENTS): try to create scope for not existing account."""
        account = str(uuid()).lower()[:30]
        scope = scope_name_generator()
        with pytest.raises(AccountNotFound):
            self.scope_client.add_scope(account, scope)

    def test_create_scope_duplicate(self):
        """ SCOPE (CLIENTS): try to create a duplicate scope."""
        account = 'jdoe'
        scope = scope_name_generator()
        self.scope_client.add_scope(account, scope)
        with pytest.raises(Duplicate):
            self.scope_client.add_scope(account, scope)

    def test_list_scopes(self):
        """ SCOPE (CLIENTS): try to list scopes for an account."""
        account = 'jdoe'
        scope_list = [scope_name_generator() for _ in range(5)]
        for scope in scope_list:
            self.scope_client.add_scope(account, scope)

        svr_list = self.scope_client.list_scopes_for_account(account)

        for scope in scope_list:
            if scope not in svr_list:
                assert False

    def test_list_scopes_account_not_found(self):
        """ SCOPE (CLIENTS): try to list scopes for a non existing account."""
        account = account_name_generator()
        with pytest.raises(AccountNotFound):
            self.scope_client.list_scopes_for_account(account)

    def test_list_scopes_no_scopes(self):
        """ SCOPE (CLIENTS): try to list scopes for an account without scopes."""
        account = account_name_generator()
        self.account_client.add_account(account, 'USER', 'rucio@email.com')
        with pytest.raises(ScopeNotFound):
            self.scope_client.list_scopes_for_account(account)
