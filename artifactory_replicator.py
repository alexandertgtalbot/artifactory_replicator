#!/bin/env python

#
#    Artifactory Replicator - Replicate (pre-cache) large repositories.
#    Copyright (C) 2019 Alexander T. G. Talbot
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import json
import requests
import tempfile
import shutil
from threading import Thread
from sortedcontainers import SortedSet
from Queue import Queue

import os
import sys
import logging

from config_manager import ConfigManager

import argparse

class Repository:
  def __init__( self,
                username = None,
                password = None,
                repository_name = None,
                base_url = None,
                filename = None,
                delete_temp_directory = True,
                max_concurrent_threads = 10,
                verify_ssl = True ):

    # A privileged user able to perform the following API requests:
    # 1. File list - https://www.jfrog.com/confluence/display/RTF/Artifactory+REST+API#ArtifactoryRESTAPI-FileList
    # 2. Artifact sync download - https://www.jfrog.com/confluence/display/RTF/Artifactory+REST+API#ArtifactoryRESTAPI-ArtifactSyncDownload
    self.username = username
    self.password = password

    # The base URL used to perform full API endpoint-specific REST calls.
    self.base_url = base_url

    self.repository_name = repository_name

    # File listing API endpoint.
    # todo - make parameters configurable, e.g. mdTimestamps = 0|1.
    self.file_listing_url_append = '/?list&deep=1&listFolders=0&mdTimestamps=0&statsTimestamps=0&includeRootPath=1'
    self.file_list_endpoint = self.base_url + '/api/storage/' + self.repository_name + self.file_listing_url_append

    # Artifact sync API endpoint components.
    self.artifact_sync_url_append = '?content=none'
    self.artifact_sync_endpoint = self.base_url + '/api/download/' + self.repository_name

    # A temporary directory for downloading the remote file listings.
    self.temp_directory = tempfile.mkdtemp(prefix='artifactory_replicator-')

    # A bool used to trigger temp file/dir cleanup post-replication.
    self.delete_temp_directory = delete_temp_directory

    # Assign a reference to an existing and previously used file listing,
    # useful for testing purposes.
    self.repository_list_temp_file = filename

    # A count of actual files in self.repo_data['files'] (not folders).
    self.file_count = None

    # A count of folders in self.repo_data['files'] (not files).
    self.folder_count = None

    # A reference to the source Repository to replicate from.
    self.source_repository = None

    # A set of files (uris) missing from self.repo_data['files'] as compared
    # to another instance of Repository.
    self.missing_files_set = None

    # The maximum number of threads to be used to run the repository sync/replication.
    self.max_concurrent_threads = max_concurrent_threads

    # Whether or not to verify signed certs, set to False if self-signed or connecting to IP/localhost.
    self.verify_ssl = verify_ssl


  def delete_temporary_directories(self):
    # Delete the temporary directory if set to do so.
    if self.delete_temp_directory == True:
      try:
        shutil.rmtree(self.temp_directory)
      except OSError as exception:
        logger.error(str(exception))


  # Set a reference repository to replicate from.
  def set_source_repository(self, source_repository):
    logger.debug("Checking that 'source_repository' is of type 'Repository'")
    if isinstance(source_repository, Repository):
      logger.debug("Confirmed that 'source_repository' is of type 'Repository'")
      self.source_repository = source_repository
    else:
      raise TypeError("'source_repository' is not of type 'Repository', type = " + str(type(source_repository)))

  # Return a reference to a Repository to replicate from,
  # if not a valid reference raise an exception.
  def __get_source_repository(self):
    # todo - test for valid reference.
    if isinstance(source_repository, Repository):
      logger.debug("Confirmed that 'source_repository' is of type 'Repository'")
      return self.source_repository
    else:
      raise TypeError("'source_repository' is not of type 'Repository', type = " + str(type(self.source_repository)))


  # Perform a file listing REST call writing the JSON response to a temporary file.
  def __download_repository_list(self):
    # Set a full path for the artifact listing temp file.
    self.repository_list_temp_file = self.temp_directory + '/' + self.repository_name + '.json'
    logger.debug('self.repository_list_temp_file = ' + self.repository_list_temp_file)

    # Perform the file list REST call.
    my_request = requests.get( self.file_list_endpoint,
                               stream = True,
                               auth=(self.username, self.password),
                               verify = self.verify_ssl )

    # Stream the ouput of the REST call above directly into the temp file.
    with open(self.repository_list_temp_file, 'wb') as fh:
      shutil.copyfileobj(my_request.raw, fh)


  # Create light weight sets for the source and destination artifact lists and produce a differential sorted set
  # with respect to the source. Only artifacts missing in the destination repository and present in the source
  # will later be sync'd.
  def __create_missing_files_set(self):
    source_set = set()
    for reference_file in source_repository.files_and_folders():
      if reference_file['folder'] == False:
        source_set.add(reference_file['uri'])

    destination_set = set()
    for my_file in self.files_and_folders():
      if reference_file['folder'] == False:
        destination_set.add(my_file['uri'])

    # A sorted set to be used to trigger missing artifact/file syncs.
    self.missing_files_set = SortedSet(source_set.difference(destination_set))


  # Load JSON from file if provided or download if absent.
  def load_repository_list(self):
    if self.repository_list_temp_file == None:
      self.__download_repository_list()
    else:
      logger.debug(self.repository_list_temp_file + ' has already been provided.')

    logger.debug('Loading ' + self.repository_list_temp_file + '.')
    if os.path.isfile(self.repository_list_temp_file):
      repo_file = open(self.repository_list_temp_file)
      self.repo_data = json.loads( repo_file.read() )
      repo_file.close()
      self.__set_file_and_folder_counts()
    else:
      logger.error(repository_list_temp_file + ' is not a file.')


  # Carry out all steps required for replication. This would be triggered by the destination repository instance.
  def run_replication(self):
    try:
      logger.info('Obtaining source/destination file lists')
  
      # To be used with the for loop below to wait for each thread's
      # completion (each in turn).
      threads_list = []
  
      logger.debug('Starting thread 1')
      # This thread will trigger a file listing for the source repository.
      t = Thread(target=self.__get_source_repository().load_repository_list, args=())
      threads_list.append(t)
      t.start()
  
      logger.debug('Starting thread 2')
      # This thread will trigger a file listing for this instance reprasentative
      # of the destination repository.
      t = Thread(target=self.load_repository_list, args=())
      threads_list.append(t)
      t.start()
  
      logger.debug('Waiting for file listings to complete')
      for t in threads_list:
        t.join()
      logger.info('File listings complete')
  
      logger.info('Determining missing content')
      self.__create_missing_files_set()
      logger.info('source = ' + str(self.__get_source_repository().number_of_files()) + ' files, ' + str(self.__get_source_repository().number_of_folders()) + ' folders.')
      logger.info('destination = ' + str(self.number_of_files()) + ' files, ' + str(self.number_of_folders()) + ' folders.')
  
      self.trigger_missing_artifact_sync()

    except Exception as exception:
      logger.error(str(exception))

    finally:
      self.__get_source_repository().delete_temporary_directories()
      self.delete_temporary_directories()

  def files_and_folders(self):
    return self.repo_data['files']

  def number_of_files(self):
    return self.file_count

  def number_of_folders(self):
    return self.folder_count

  def trigger_missing_artifact_sync(self):
    sync_queue = Queue(maxsize = 0)
    concurrent_threads = min(len(self.missing_files_set), self.max_concurrent_threads)
    results = [{} for x in self.missing_files_set]

    # Populate queue.
    for i in range(len(self.missing_files_set)):
      sync_queue.put((i, self.missing_files_set[i]))

    for i in range(concurrent_threads):
      logger.debug('Starting thread ' + str(i))
      worker = Thread(target=self.__sync_artifact, args=(i,sync_queue,results))
      worker.setDaemon(True)
      worker.start()

    # Wait for all threads to complete.
    sync_queue.join()
    logger.info('All tasks completed.')

  # Performs an Artifactory sync call.
  def __sync_artifact(self, thread_id, sync_queue, results):
    while not sync_queue.empty():
      job = sync_queue.get()

      url = self.artifact_sync_endpoint + job[1] + self.artifact_sync_url_append
      try:
        result = requests.get( url,
                               auth=(self.username, self.password),
                               verify = self.verify_ssl )
      except:
        logger.error('Unable to connect to url - ' + url)
      else:
        results[job[0]] = 'Thread (' + str(thread_id) + ') - ' + url + ' | ' + 'status code: ' + str(result.status_code) + ' | ' + result.text
        logger.debug(results[job[0]])

      sync_queue.task_done()
    return True

  def __set_file_and_folder_counts(self):
    self.file_count = 0
    self.folder_count = 0
    for file in self.repo_data['files']:
      if file['folder'] == True:
        self.folder_count += 1
      else:
        self.file_count += 1

if __name__ == '__main__':

  

  # Load arguments
  parser = argparse.ArgumentParser(description='Run Artifactory repository replication.')

  parser.add_argument('--config',
                     default='/etc/artifactory_replicator/config.json',
                     help='The configuration file to load')

  parser.add_argument('--source-username',
                     default=None,
                     help='The username to use to replicate from the source repository')
  parser.add_argument('--source-password',
                     default=None,
                     help='The password to use to replicate from the source repository')
  parser.add_argument('--source-base-url',
                     default=None,
                     help='''The base URL to use to replicate from the source
                             repository, e.g.
                             https://artifactory.source.com/artifactory''')
  parser.add_argument('--source-repository-name',
                     default=None,
                     help='The source repository name')

  parser.add_argument('--destination-username',
                     default=None,
                     help='The username to use to replicate to the destination repository')
  parser.add_argument('--destination-password',
                     default=None,
                     help='The password to use to replicate to the destination repository')
  parser.add_argument('--destination-base-url',
                     default=None,
                     help='''The base URL to use to replicate to the destination
                             repository, e.g.
                             https://artifactory.destination.com/artifactory''')
  parser.add_argument('--destination-repository-name',
                     default=None,
                     help='The destination repository name')


  parser.add_argument('--keep-temp-directory',
                     default=False,
                     action='store_true',
                     help='Keep temporary directories (default: Delete)')
  parser.add_argument('--max-concurrent-threads',
                     default=10,
                     type=int,
                     help=''''The number of concurrent threads for replication of
                             a single repository (default: 10)''')
  parser.add_argument('--no-verify-ssl',
                     default=False,
                     action='store_true',
                     help='Disable SSL verification (default: verify)')

  parser.add_argument('--logging-path',
                     default='/var/log/artifactory_replicator/artifactory_replicator.log',
                     help='Path/file to log to (default: /var/log/artifactory_replicator/artifactory_replicator.log)')

  parser.add_argument('--debug',
                     default=False,
                     help='Enable debug logging (default: console - none, log - info )')

  parser.add_argument('--console',
                     default=False,
                     help='Enable console logging')

  parser.add_argument('--version', action='version', version='%(prog)s 0.2.0')

  args = parser.parse_args()


  # Configure logger.
  logger = logging.getLogger('artifactory_replicator')
  
  # Enable debug logging if --debug is passed else default to info level.
  logger_level = logging.DEBUG if args.debug else logging.INFO
  logger.setLevel(logger_level)
  formatter = logging.Formatter('%(asctime)s [%(name)s-%(process)d] [%(levelname)s] - %(message)s')

  # Enable console logging if --console is passed.
  if args.console:
    ch = logging.StreamHandler()
    ch.setLevel(logger_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


  # Load configs
  config_manager = None
  # Assign arguments provided or argument defaults as config defaults. This
  # approach treats the config file as the ultimate source of truth ignoring
  # command line arguments where configs are set.
  defaults = { 'source_username': args.source_username,
               'source_password': args.source_password,
               'source_base_url': args.source_base_ur,
               'source_repository_name': args.source_repository_name,
               'destination_username': args.destination_username,
               'destination_password': args.destination_password,
               'destination_base_url': args.destination_base_url,
               'destination_repository_name': args.destination_repository_name,
               # Inverse logic set for improved readability of
               # command line argument.
               'delete_temp_directory': not args.keep_temp_directory,
               'max_concurrent_threads': args.max_concurrent_threads,
               'verify_ssl': args.no_verify_ssl,
               'logging_path': args.logging_path }

  try:
    logger.info('Loading configuration ' + args.config)
    config_manager = ConfigManager(config_file_path = args.config, defaults = defaults, required = required)
    logger.info('Configuration loaded')

    # todo - set log level from config.
    # Configure file logging.
    fh = logging.FileHandler(config_manager['logging_path'])
    fh.setLevel(logger_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

  except (OSError, IOError) as exception:
    logger.error('Unable to load config')


#  else:
#    # Repository instances representative of the source and destination repositories.
#    source_repository      = Repository( username               = config_manager['source_username'],
#                                         password               = config_manager['source_password'],
#                                         repository_name        = config_manager['source_repository_name'],
#                                         base_url               = config_manager['source_base_url'],
#                                         delete_temp_directory  = config_manager['delete_temp_directory'],
#                                         max_concurrent_threads = config_manager['max_concurrent_threads'],
#                                         #filename = '/tmp/artifactory_replicator-b8eJCc/example_repository-local.json',
#                                         verify_ssl             = config_manager['verify_ssl'] )
#    destination_repository = Repository( username               = config_manager['destination_username'],
#                                         password               = config_manager['destination_password'],
#                                         repository_name        = config_manager['destination_repository_name'],
#                                         base_url               = config_manager['destination_base_url'],
#                                         delete_temp_directory  = config_manager['delete_temp_directory'],
#                                         max_concurrent_threads = config_manager['max_concurrent_threads'],
#                                         #filename = '/tmp/artifactory_replicator-Nayx5r/example_repository-remote.json',
#                                         verify_ssl             = config_manager['verify_ssl'] )
#
#    try:
#      # Pass a source repository reference to the destination instance.
#      destination_repository.set_source_repository(source_repository)
#    except TypeError as exception:
#        logger.error(str(exception))
#    else:
#      logger.info('Initiating replication')
#      try:
#        destination_repository.run_replication()
#      except Exception as exception:
#        logger.error(str(exception))

