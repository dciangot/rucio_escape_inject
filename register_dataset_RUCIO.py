#! /bIN/env python
"""
Command line tool for registering a CMS dataset into rucio
"""

from __future__ import absolute_import, division, print_function

import os
import zlib
import logging
import json
import subprocess
import string
from argparse import ArgumentParser

from gfal2 import Gfal2Context
from rucio.client import uploadclient
from rucio.client.client import Client
from rucio.common.exception import (AccountNotFound, DataIdentifierNotFound, AccessDenied, DuplicateRule,
                                                                        DataIdentifierAlreadyExists, DuplicateContent, InvalidRSEExpression,
                                                                        UnsupportedOperation, FileAlreadyExists, RuleNotFound)


if __name__ == "__main__":

    parser = ArgumentParser(description='Arguments for file Rucio upload')
    parser.add_argument('dataset', type=str, help='dataset name')
    parser.add_argument('--source', type=str, help='Source pfn')
    parser.add_argument('--name', type=str, help='Rucio name')
    parser.add_argument('--destination', type=str, help='Rucio RSE destination')
    parser.add_argument('--account', type=str, help='Rucio account')
    parser.add_argument('--scope', type=str, help='Rucio scope')

    args = parser.parse_args()

    rucioClient = Client(account=args.account,
                         auth_type='x509',
                         creds={"client_cert": '/tmp/proxy', "client_key": '/tmp/proxy'})

    logger = logging.getLogger('simple_example')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    uploadCli = uploadclient.UploadClient(rucioClient, logger=logger)

    try:
        rucioClient.add_dataset(args.scope, args.dataset)
    except DataIdentifierAlreadyExists:
        pass
    except Exception as ex:
        raise ex

    ctx = Gfal2Context()

    listdir = ctx.listdir(args.source)

    files = []
    dids = []
    for f in listdir[:10]:
      sc_file = args.source + "/" + f
      name = args.name + "/" + f

      lfn = string.join(name.split('/')[1:], '/').replace("/","__")
      pfn = sc_file
      size = ctx.stat(sc_file).st_size
      checksum = ctx.checksum(sc_file, 'adler32')
      md5 = None #ctx.checksum(args.source, 'md5')

      files.append({'path': pfn,
                          'bytes': size,
                          'adler32': checksum,
                          'md5': md5,
                          'rse': args.destination,
                          'scope': args.scope,
                          'did_scope': args.scope,
                          'basename': f,
                          'dirname': string.join(pfn.split('/')[:-1], '/'),
                          'did_name': lfn,
                          'register_after_upload': True,
                          'lifetime': 6000,
                          'meta': {},
                          'force_scheme': None
                          })
      dids.append({'name': lfn, 'scope': args.scope})

    print(files)

    uploadCli.upload(files)
    rucioClient.attach_dids(args.scope, args.dataset, dids=dids, rse=args.destination)

