#! /Bin/env python
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

    """
    for blk in blocks:
        cmd = ["dasgoclient", "--query", "site block=%s instance=prod/phys03" % blk, "-json"]
        location_stdout = subprocess.check_output(cmd)

        locations = [x['site'][0]['name'] for x in json.loads(location_stdout)]

        lfns = [ f["name"] for f in files if f['block.name'] == blk ]
        file_sizes = [ f["size"] for f in files if f['block.name'] == blk ]
        checksums = [ f["adler32"] for f in files if f['block.name'] == blk ]
        md5s = [ f["md5"] for f in files if f['block.name'] == blk ]


        files = []
        dids = []

        for lfn, size, checksum, md5 in zip (lfns, file_sizes, checksums, md5s):
            src_pfn = rucioClient.lfns2pfns(locations[0], [args.scope+":"+lfn])
            src_dir = rucioClient.lfns2pfns(locations[0], [args.scope+":"+os.path.dirname(lfn)])
            dst_pfn = rucioClient.lfns2pfns(args.destination, [args.scope+":"+lfn.replace("/store/user/", "/store/temp/user/").replace("/store/group", "/store/temp/user/"+args.account)])
            print(md5)
            files.append({'path': src_pfn[args.scope+":"+lfn],
                          'bytes': size,
                          'adler32': checksum,
                          'md5': md5,
                          'rse': args.destination+'_Temp',
                          'scope': args.scope,
                          'did_scope': args.scope,
                          'basename': os.path.basename(lfn),
                          #'basename': src_pfn[args.scope+":"+lfn],
                          #'dirname': os.path.dirname(lfn.replace('/store/user/', '/store/user/rucio/').replace("/store/group", "/store/user/rucio/"+args.account)),
                          'dirname': src_dir[args.scope+":"+os.path.dirname(lfn)],
                          'did_name': lfn.replace('/store/user/', '/store/user/rucio/').replace("/store/group", "/store/user/rucio/"+args.account),
                          'pfn': dst_pfn[args.scope+":"+lfn.replace("/store/user/", "/store/temp/user/").replace("/store/group", "/store/temp/user/"+args.account)],
                          'register_after_upload': True,
                          'lifetime': 6000,
                          'meta': {},
                          'force_scheme': None
                          })
            dids.append({
                'name': lfn.replace('/store/user/', '/store/user/rucio/').replace("/store/group/", "/store/user/rucio/"+args.account)
            })

        print(files[0])


        uploadCli.upload(files)
        rucioClient.attach_dids(args.scope, args.dataset, dids=dids, rse=args.destination)

    """
    '''
    {'check_sum': '827565343', 'adler32': '4b5cb5ea', 'block_id': 3983783, 'event_count': 3
    'file_type': 'EDM', 'create_by': None, 'branch_hash_id': None, 'last_modified_by': 'dciangot', 'creation_date': None,
    'block_name': '/BstarToGJ_M-1500_f-0p5_Tune4C_8TeV-pythia8/dciangot-CRAB3_test_104-cf8e2dadd96bfd83ba0630742b21d982/USER#244c29dd-682f-4c68-b6e4-16a42519171d',
    'is_file_valid': 1, 'md5': None, 'logical_file_name': '/store/user/dciangot/BstarToGJ_M-1500_f-0p5_Tune4C_8TeV-pythia8/CRAB3_test_104/160721_093609/0000/output_6.root',
    'file_size': 1207578, 'last_modification_date': 1469095433, 'dataset_id': 2397303,
    'dataset': '/BstarToGJ_M-1500_f-0p5_Tune4C_8TeV-pythia8/dciangot-CRAB3_test_104-cf8e2dadd96bfd83ba0630742b21d982/USER',
    'file_type_id': 1, 'auto_cross_section': None, 'file_id': 197500854}]
    '''
