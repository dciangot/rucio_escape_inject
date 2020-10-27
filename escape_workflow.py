
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
import shutil
from argparse import ArgumentParser

from rucio.client import downloadclient
from rucio.client.client import Client
from rucio.common.exception import (AccountNotFound, DataIdentifierNotFound, AccessDenied, DuplicateRule,
                                                                        DataIdentifierAlreadyExists, DuplicateContent, InvalidRSEExpression,
                                                                        UnsupportedOperation, FileAlreadyExists, RuleNotFound)


if __name__ == "__main__":

    parser = ArgumentParser(description='Arguments for file Rucio upload')
    parser.add_argument('dataset', type=str, help='dataset name')
    #parser.add_argument('--source', type=str, help='Source pfn')

    args = parser.parse_args()



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

    rucioClient = Client(account=os.environ["RUCIO_ACCOUNT"],
                         auth_type='x509',
                         creds={"client_cert": '/tmp/proxy', "client_key": '/tmp/proxy'})
    #logger.info(rucioClient.whoami())

    downloadCli = downloadclient.DownloadClient(rucioClient, logger=logger)

    files_gen = rucioClient.list_files(args.dataset.split(":")[0], args.dataset.split(":")[1] )
    files = [{'scope': f['scope'], 'name': f['name']} for f in files_gen]
    #logger.info(files)

    replicas = [r for r in rucioClient.list_replicas(files)]
    #logger.info(replicas[0])
    pfns_for_file = []
    for r in replicas:
        pfns = []
        good_sites = []
        for site, state in r['states'].items():
            if state == "AVAILABLE":
                good_sites.append(site)
        #logger.info("GOOD SITES: %s" % good_sites)

        for site, pfn in r['rses'].items():
            if site in good_sites:
                pfns.append(pfn[0])
        pfns_for_file.append(pfns)

    inputs = zip(files, pfns_for_file)

    shutil.rmtree("/eos/user/d/dciangot/" + inputs[0][0]['scope'], ignore_errors=True)
    shutil.rmtree("/eos/user/d/dciangot/escape_outputs", ignore_errors=True)
    to_download = [{'did': i[0]['scope']+":"+i[0]['name'], 'base_dir': "/eos/user/d/dciangot/"} for i in inputs]
    downloadCli.download_dids(to_download)

    os.mkdir("/eos/user/d/dciangot/escape_outputs")
    import ROOT
    #/eos/user/d/dciangot/CMS_INFN_DCIANGOT/opendata__cms__Run2010B__Mu__AOD__Apr21ReReco-v1__0000__00459D48-EB70-E011-AF09-90E6BA19A252.root
    for it in inputs:
        job_input = "/eos/user/d/dciangot/" + it[0]['scope'] + "/" + it[0]['name']
        fill = ROOT.TFile(job_input)
        fill2 = fill.Cp("/eos/user/d/dciangot/escape_outputs/output__"+it[0]['name'])

