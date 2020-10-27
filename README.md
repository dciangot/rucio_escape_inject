# Rucio data-lake inject exercises

## Automatic importing of data into data-lake

- injected a dataset from existing GRID site (eos cern) into the data lake
	- changes needed to the rucio client, in order to allow this through gfal https://github.com/rucio/rucio/issues/4072
	- issue stil open but I put here the modified python source
	- this will allow to insert into the datalake any data accessible with a grid certificate and cms opendata
	- to import data import and registration is here:
```
source /cvmfs/cms.cern.ch/rucio/setup.sh
export X509_USER_PROXY=/tmp/proxy
export RUCIO_ACCOUNT=dciangot
export RUCIO_HOME=/afs/cern.ch/user/d/dciangot/public/rucio_escape
export PYTHONPATH=$PWD:$PYTHONPATH

export SOURCE=root://eospublic.cern.ch//eos/opendata/cms/Run2010B/Mu/AOD/Apr21ReReco-v1/0000/
export NAME=/opendata/cms/Run2010B/Mu/AOD/Apr21ReReco-v1/0000/

python register_dataset_RUCIO.py --source $SOURCE --name $NAME  opendata1 --account dciangot --scope CMS_INFN_DCIANGOT --destination DESY-DCACHE
```
- created a rule for data replication to CNAF
	- still in TODO: `Add replication rule based on attributes: rucio add-rule YOUREXPERIMENT_YOURINSTITUTION_YOURNAME:FILE_TO_BE_UPLOADED 1 QOS=FAST`

## Simple workflow exercise

- then run a simple workflow on this data with python `python escape_workflow.py CMS_INFN_DCIANGOT:opendata1`:
	- copyng a file locally on the worker node
	- producing a simple demo output
    - still to be registered on Rucio (TODO)
	- nothing more realistic is possible on opendata (due to slc6 and python2 required), if not with a considerably amount of effort
  
