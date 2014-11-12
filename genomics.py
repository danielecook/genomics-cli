"""Genomics


Usage:
  genomics.py setup
  genomics.py buckets (--list|--create=<name>|--delete=<name>)
  genomics.py objects <bucket> (--list|--upload=<vcf/bcf>)
  genomics.py datasets (--list [--all]|--create=<name> [--public]|--delete=<name>|--undelete=<id>)
  genomics.py variants (--import=<location> <dataset>)
  genomics.py jobs
  genomics.py query
  genomics.py bigquery --list [--dataset=<dataset>]
  genomics.py --version

asdf
asdf
Options:
  -h --help     Show this screen.
  --version     Show version.

  When importing variants, 


"""
from docopt import docopt
import yaml
import json
from tabulate import tabulate
import os
from yaml import load, dump
from pprint import pprint as pp
from collections import OrderedDict

import httplib2
import pprint
import sys

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaIoBaseUpload
import io

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def connect(service="genomics"):
  # Enter your Google Developer Project number
  FLOW = flow_from_clientsecrets('client_secrets.json',
                                 scope=['https://www.googleapis.com/auth/bigquery',
                                        'https://www.googleapis.com/auth/genomics',
                                        'https://www.googleapis.com/auth/devstorage.read_write'])
  # Authorization and credentials
  storage = Storage('credentials.dat')
  credentials = storage.get()

  if credentials is None or credentials.invalid:
    credentials = run(FLOW, storage)

  http = httplib2.Http()
  http = credentials.authorize(http)

  if service == "bigquery":
    return build('bigquery', 'v2', http=http)
  elif service == "storage":
    return build('storage','v1',http=http)
  else:
    return build('genomics', 'v1beta2', http=http)


def submit(action):
  try:
    return action.execute()
  except HttpError as err:
    for error in json.loads(err.content)["error"]["errors"]:
      message = "\n{color}{reason}{retcolor} - {message}\n"
      print message.format(color=bcolors.FAIL,reason=error["reason"],message=error["message"],retcolor=bcolors.ENDC)

  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")

def list_datasets(new_dataset_id=None):
  print("")
  datasets = submit(genomics.datasets().list(projectNumber=PROJECT_NUMBER))
  if 'datasets' in datasets:
    datasets = datasets["datasets"]
    if new_dataset_id is not None:
      for k,v in enumerate(datasets):
          if new_dataset_id == v["id"]:
            datasets[k][""] = "New Dataset"
    print tabulate(datasets, headers="keys")
    print("")
  else:
    print("No Datasets found.")

def list_buckets(new_bucket_id=None):
  bucketlist = submit(cloudstorage.buckets().list(project=PROJECT_NUMBER))["items"]
  bucketlist_keys = ["id", "name", "timeCreated"]
  # Filter for a few select columns.
  bucketlist = [{k:v for k,v in x.items() if k in bucketlist_keys} for x in bucketlist]
  if new_bucket_id is not None:
    new_bucket_id = new_bucket_id["id"]
    for k,v in enumerate(bucketlist):
      if new_bucket_id == v["id"]:
        bucketlist[k][""] = "New Bucket"
  print("")
  print(tabulate(bucketlist,headers="keys"))
  print("")

def list_objects(new_object_id=None):
  objects = submit(cloudstorage.objects().list(bucket=arguments["<bucket>"]))
  objects = objects["items"]
  object_list_keys = ["name", "updated", "size","etag"]
  objects = [{k:v for k,v in x.items() if k in object_list_keys} for x in objects]
  if new_object_id is not None:
    new_object_id = new_object_id["etag"]
    for k,v in enumerate(objects):
      if new_object_id == v["etag"]:
        objects[k][""] = "New Object"
  print("")
  print("BUCKET: {bucket}".format(bucket=arguments["<bucket>"]))
  print tabulate(objects, headers="keys")
  print("")

if __name__ == '__main__':
  arguments = docopt(__doc__, version='Google Genomics 0.1')
  print arguments
  # Get Project Number
  if arguments["setup"] == True or not os.path.isfile("config.yaml"):
    config = {}
    config["PROJECT_NUMBER"] = raw_input("Project ID: ")
    f = open("config.yaml",'w')
    f.write(yaml.dump(config))
  else:
    try:
      PROJECT_NUMBER = os.environ["PROJECT_NUMBER"]
    except:
      if os.path.isfile("config.yaml"):
        config = yaml.load(open("config.yaml",'r'))
        PROJECT_NUMBER = config["PROJECT_NUMBER"]
      else:
        config = {}
        config["PROJECT_NUMBER"] = raw_input("Project Number: ")
        f = open("config.yaml",'w')
        f.write(yaml.dump(config))

  #==========#
  # Datasets #
  #==========#

  if arguments["datasets"] == True:
    genomics = connect()
    if arguments["--all"] and arguments["--list"] == True:
      variantSets = []
      for dataset in submit(genomics.datasets().list(projectNumber=PROJECT_NUMBER))["datasets"]:
        # Format variant Sets
        variantSets.extend(submit(genomics.variantsets().search(body={"datasetIds":[dataset["id"]]}))["variantSets"])
        variantSets[-1]["name"] = dataset["name"]
      variantSets_keys = ["datasetId", "id", "referenceBounds","name"]
      # Filter for a few select columns.
      variantSets = [{k:v for k,v in x.items() if k in variantSets_keys} for x in variantSets]
      # Format reference Bounds 
      for i,v in enumerate(variantSets): 
        variantSets[i]["referenceName"] = ', '.join([x["referenceName"] for x in v["referenceBounds"]])
        del variantSets[i]["referenceBounds"]
      print(tabulate(variantSets, headers="keys"))
    elif arguments["--list"] == True:
      list_datasets()
    elif arguments["--create"] is not None:
      isPublic = False
      if arguments["--public"] == True:
        isPublic = True
      new_dataset = submit(genomics.datasets().create(body={"isPublic":isPublic, 
                                       "name":arguments["--create"],
                                       "projectNumber":PROJECT_NUMBER}))

      list_datasets(new_dataset_id=new_dataset["id"])
    elif arguments["--delete"] is not None:
      datasets = submit(genomics.datasets().list(projectNumber=PROJECT_NUMBER))
      datasets = datasets["datasets"]
      datasets = [x for x in datasets if x["name"] == arguments["--delete"] or x["id"] == arguments["--delete"]]
      if len(datasets) == 0:
        print("Dataset not found.")
      elif len(datasets) > 1:
        delete_all = raw_input("There are %s datasets that have the name '%s', delete all? (y/n)" % (len(datasets),arguments["--delete"]))
        if delete_all == "y":
          for i in datasets:
             submit(genomics.datasets().delete(datasetId=i["id"]))
        else:
          print("No datasets were deleted.")
      else:
        datasetId = datasets[0]["id"]
        deleteReply = submit(genomics.datasets().delete(datasetId=datasetId))
      list_datasets()
    elif arguments["--undelete"] is not None:
      submit(genomics.datasets().undelete(datasetId=arguments["--undelete"]))
      list_datasets()
    
  #=========#
  # Buckets #
  #=========#

  elif arguments["buckets"] == True:
    cloudstorage = connect(service="storage")
    if arguments["--list"] == True:
      list_buckets()
    elif arguments["--create"] is not None:
      bucket_id = submit(cloudstorage.buckets().insert(project=PROJECT_NUMBER,body={"name":arguments["--create"]}))
      list_buckets(new_bucket_id=bucket_id)
    elif arguments["--delete"] is not None:
      submit(cloudstorage.buckets().delete(bucket=arguments["--delete"]))
      list_buckets()
  
  elif arguments["objects"] == True:
    cloudstorage = connect(service="storage")
    if arguments["--list"] == True:
      list_objects()
    elif arguments["--upload"] is not None:

      # URI scheme for Google Cloud Storage.
      GOOGLE_STORAGE = 'gs'
      # URI scheme for accessing local files.
      LOCAL_FILE = 'file'
      vcf = arguments["--upload"]
      bucket = arguments["<bucket>"]

      # The BytesIO object may be replaced with any io.Base instance.
      media = MediaIoBaseUpload(io.open(vcf,'r'), 'text/plain')
      req = cloudstorage.objects().insert(
              bucket=bucket,
              name=vcf,
              #body=object_resource,     # optional
              media_body=media)
      resp = req.execute()
      #print json.dumps(resp, indent=2)
      list_objects(new_object_id=resp)

  #==========#
  # Variants #
  #==========#

  elif arguments["variants"] == True:
    if arguments["--import"] is not None:
      genomics = connect()
      # Look up variantSetId if not specified as number.
      if arguments["<dataset>"].isdigit():
        variantSetId = arguments["<dataset>"]
      else:
        datasets = genomics.datasets().list(projectNumber=PROJECT_NUMBER).execute()["datasets"]
        id_from_name = [x for x in datasets if x["name"] == arguments["<dataset>"]]
        if len(id_from_name) == 1:
          variantSetId = id_from_name[0]["id"]
        else:
          print("Found multiple datasets with the name %s. Nothing was imported" % arguments["<dataset>"])
      jobid = genomics.variantsets().importVariants(variantSetId=variantSetId,body={"sourceUris":[arguments["--import"]], "format":"vcf"}).execute()
      print jobid

  #======#
  # Jobs #
  #======#

  elif arguments["jobs"] == True:
    genomics = connect()
    jobs = genomics.jobs().search(body={"projectNumber":PROJECT_NUMBER}).execute()["jobs"]
    job_keys = ["id","status", "errors", "request"]
    jobs = [{k:v for k,v in x.items() if k in job_keys} for x in jobs]
    print tabulate(jobs)

  """
  try:
    datasets = bigquery_service.datasets()
    listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
    print 'Dataset list:'
    pprint.pprint(listReply)

  except HttpError as err:
    print 'Error in listDatasets:', pprint.pprint(err.content)

  except AccessTokenRefreshError:
    print ("Credentials have been revoked or expired, please re-run"
           "the application to re-authorize")
  """


