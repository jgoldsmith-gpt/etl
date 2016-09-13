import requests
import json
import time
import base64

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

class LinkResponse(object):
  """Class to store essential response data from a WattzOn Link API request"""

  def __init__(self, response):
    self.status_code = response.status_code
    self.content_type = response.headers.get('content-type')
    self.data = response.text

    if self.success():
      if self.content_type == 'application/octet-stream':
        self.data = response.content
      elif self.content_type == 'application/pdf':
        self.data = response.content
      else:
        self.data = response.json()

  def success(self):
    """Whether the response was a successful one (200s status code) 
    or there was an error (400s and 500s)

    Returns
    -------
    bool
      False if status code between 400-600, True otherwise
    """
    if 400 <= self.status_code < 600:
      return False
    return True


class BaseClient(object):
  """Base Class for WattzOn client with basic functionality. Only 1 client, LinkClient, but could be extended if more introduced."""

  def __init__(self, host='localhost', port=None, protocol='https', certificate=None, key=None, **kwargs):
    """Initialize the host url with port/protocol, default to localhost."""
    self.host = host
    self.port = port
    self.protocol = protocol
    self.__url = '{}://{}'.format(self.protocol, self.host)
    self.__certificate = certificate
    self.__key = key
    self.kwargs = kwargs

    if self.port is not None:
      self.__url += ':{}'.format(self.port)

  @property
  def url(self):
    """Returns URL built with host, port, and protocol"""
    return self.__url

  def build_uri(self, endpoint='/'):
    """Builds complete url with endpoint, host, port, and protocol"""
    if endpoint[-1] != '/':
      endpoint += '/'
    return self.url + endpoint

  def post(self, endpoint, data=None):
    """post for WattzOn endpoints"""
    url = self.build_uri(endpoint)
    payload = data
    response = requests.post(url, data=json.dumps(payload), cert=(self.__certificate, self.__key), **self.kwargs)
    return LinkResponse(response)

  def get(self, endpoint, data=None):
      """get for WattzOn endpoints"""
      url = self.build_uri(endpoint)
      query = data
      response = requests.get(url, params=query, cert=(self.__certificate, self.__key), **self.kwargs)
      return LinkResponse(response)

  def put(self, endpoint, data=None):
      """put for WattzOn endpoints"""
      url = self.build_uri(endpoint)
      payload = data
      response = requests.put(url, data=json.dumps(payload), cert=(self.__certificate, self.__key), **self.kwargs)
      return LinkResponse(response)

  def delete(self, endpoint, data=None):
      """delete for WattzOn endpoints"""
      url = self.build_uri(endpoint)
      payload = data
      response = requests.delete(url, data=json.dumps(payload), cert=(self.__certificate, self.__key), **self.kwargs)
      return LinkResponse(response)


class LinkAPIClient(BaseClient):
  """WattzOn Link API 4.0 client - based on API WattzOn API docs"""

  def __init__(self, **kwargs):
    super(LinkAPIClient, self).__init__(**kwargs)
    self.key = None

  def echo(self, string):
    """Calls the echo service, mostly for testing.

    Arguments:
    string -- the message that should be returned by echo
    """
    endpoint = '/link/4.0/echo/'
    data = {
      "string": string
    }

    return self.post(endpoint, data)

  def profile_list(self, id=None, provider=None, start_date=None, end_date=None, tags=None):
    """List profiles

    Keyword Arguments:
    id -- ID to filter (default None)
    provider -- provider id to filter (default None)
    start_date -- start date to filter (default None)
    end_date -- end date to filter (default None)
    tags -- tags to filter, can be a list or comma separated string
    """
    endpoint = '/link/4.0/profiles/'
    data = {}

    if id:
      data.update({"id": id})

    if provider:
      data.update({"provider": provider})

    if start_date:
      data.update({"start_date": start_date})

    if end_date:
      data.update({"end_date": end_date})

    if not tags:
      pass
    elif isinstance(tags, str):
      # assume comma separated
      data.update({"tags": tags})
    elif hasattr(tags, '__iter__'):
      # assume list
      data.update({"tags": ",".join(tags)})
    else:
      raise TypeError("tags argument must be string or list of strings")

    return self.get(endpoint, data)

  def profile_get(self, profile_id):
    """Returns given profile

    Arguments:
    profile_id -- ID of profile to get
    """
    endpoint = '/link/4.0/profiles/{}/'.format(profile_id)
    return self.get(endpoint)

  def profile_create(self, provider_id, zipcode, label=None):
    """Create a profile

    Arguments:
    provider_id -- provider id that will be associated to the profile
    zipcode -- provider zipcode that will be associated to the profile

    Keyword Arguments:
    label -- label to identify the profile (default None)
    """
    endpoint = '/link/4.0/profiles/'
    data = {
      "provider_id": provider_id,
      "zipcode": zipcode
    }

    if label:
      data.update({"label": label})

    return self.post(endpoint, data)

  def profile_change(self, profile_id, provider_id=None, zipcode=None, label=None):
    """Change profile info

    Arguments:
    profile_id -- profile to change

    Keyword Arguments:
    provider_id -- the new provider id (default None)
    zipcode -- the new zipcode (default None)
    label -- the new label to identify the profile (default None)
    """
    endpoint = '/link/4.0/profiles/{}/'.format(profile_id)
    data = {}

    if profile_id:
      data.update({"provider_id": provider_id})

    if zipcode:
      data.update({"zipcode": zipcode})

    if label:
      data.update({"label": label})

    return self.put(endpoint, data)

  def profile_delete(self, profile_id):
    """Deletes profile

    Arguments:
    profile_id -- profile to delete
    """
    endpoint = '/link/4.0/profiles/{}/'.format(profile_id)

    return self.delete(endpoint)

  def key_get(self):
    """Returns public transmission key to encyrpt sensitive fields.
    To add sensitive fields you can also use profile_sensitive_add()
    """
    endpoint = '/link/4.0/profiles/keys/'

    return self.get(endpoint)

  def _encrypt_value(self, value):
    """Helper function to encrypt sensitive fields"""
    key_query = None

    if not self.key:
      key_query = self.key_get()

    if self.key and self.key['expires'] < time.time(): # check key expired
      key_query = self.key_get()

    if key_query:
      if key_query.success():
        self.key = key_query.data
      if not key_query.success():
        raise Exception('Error while retrieving encryption key.')

    public_key = RSA.importKey(self.key['public_key'])
    cypher = PKCS1_OAEP.new(public_key)
    encrypted_data = base64.b64encode(cypher.encrypt(value.encode()))

    return encrypted_data.decode()

  def profile_sensitive_list(self, profile_id):
    """Lists sensitive fields on the profile

    Arguments:
    profile_id -- profile to list sensitive fields
    """
    endpoint = '/link/4.0/profiles/{}/sensitive/'.format(profile_id)

    return self.get(endpoint)

  def profile_sensitive_add(self, profile_id, field, value):
    """Add a sensitive field to the profile

    Arguments:
    profile_id -- profile to add sensitive field
    field -- name of the field to add
    value -- value of the field to add
    """
    endpoint = '/link/4.0/profiles/{}/sensitive/'.format(profile_id)

    if not isinstance(value, str):
      raise AttributeError("Sensitive value must be string, not {}".format(type(value)))

    data = {
      "field": field,
      "value": self._encrypt_value(value)
    }

    return self.post(endpoint, data)

  def profile_sensitive_change(self, profile_id, field, value):
    """Change a sensitive field for the profile

    Arguments:
    profile_id -- profile to change sensitive field
    field -- name of field to change
    value -- value to change the field to
    """
    endpoint = '/link/4.0/profiles/{}/sensitive/'.format(profile_id)
    data = {
      "field": field,
      "value": self._encrypt_value(value)
    }

    return self.put(endpoint, data)

  def profile_sensitive_delete(self, profile_id, field):
    """Deletes a sensitive field for the profile

    Arguments:
    profile_id - profile to delete sensitive field
    field - field to delete
    """
    endpoint = '/link/4.0/profiles/{}/sensitive/{}/'.format(profile_id, field)

    return self.delete(endpoint)

  def profile_tag_list(self, profile_id):
    """Lists tags associated with profile

    Arguments:
    profile_id -- profile to list the tags
    """
    endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)

    return self.get(endpoint)

  def profile_tag_add(self, profile_id, name):
    """Adds tag to profile

    Arguments:
    profile_id -- profile to add tag
    name -- name of tag to add
    """
    endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)
    data = {
      "name": name
    }

    return self.post(endpoint, data)

  def profile_tag_delete(self, profile_id, name):
    """Deletes tag from profile

    Arguments:
    profile_id -- profile to delete tag
    name -- name of tag to delete
    """
    endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)
    data = {
      "name": name
    }

    return self.delete(endpoint, data)

  def zip_info(self, zipcode):
    """Returns zipcode info

    Arguments:
    zipcode -- zipcode to search
    """
    endpoint = '/link/4.0/zips/{}/'.format(zipcode)

    return self.get(endpoint)

  def utility_find(self, zipcode=None, name=None, type=None):
    """Returns utility id for given arguments

    Keyword Arguments:
    zipcode -- zipcode of the provider (default None)
    name -- name of the provider (default None)
    type -- type of the provider, e.g. 'gas' (default None)
    """
    endpoint = '/link/4.0/utilities/'
    data = {}

    if zipcode:
      data.update({"zipcode": zipcode})

    if name:
      data.update({"name": name})

    if type:
      data.update({"type": type})

    return self.get(endpoint, data)

  def utility_info(self, provider_id):
    """Returns info for utility/provider

    Arguments:
    provider_id -- provider to search
    """
    endpoint = '/link/4.0/utilities/{}/'.format(provider_id)

    return self.get(endpoint)

  def job_start(self, profile_id, mode='interval'):
    """Starts a new job for the profile

    Arguments:
    profile_id -- profile to run job

    Keyword Arguments:
    mode -- mode of the WattzOn agent, e.g. 'bill' (default 'interval')
    """
    endpoint = '/link/4.0/jobs/'
    data = {
      "profile_id": profile_id,
      "mode": mode
    }

    return self.post(endpoint, data)

  def job_status(self, job_id):
    """Get status of job

    Arguments:
    job_id -- job to get status of
    """
    endpoint = '/link/4.0/jobs/{}/'.format(job_id)

    return self.get(endpoint)

  def bill_get(self, profile_id, job_id=None):
    """Returns bill details

    Arguments:
    profile_id -- profile to get bills

    Keyword Arguments:
    job_id -- job id that extracted the bill (default None)
    """
    endpoint = '/link/4.0/data/bills/{}/'.format(profile_id)
    data = {}

    if job_id:
      data.update({"job_id": job_id})

    return self.get(endpoint, data)

  def bill_file(self, profile_id, file_id):
    """Get bill pdf - file id can be found in bill_get()
    This doesn't actually work - would need to be properly handled as pdf download

    Arguments:
    profile_id -- profile that owns the bill pdf
    file_id -- file id to retrieve
    """
    endpoint = '/link/4.0/data/bills/{}/{}/pdf/'.format(profile_id, file_id)

    return self.get(endpoint)

  def interval_list(self, profile_id):
    """List intervals available for the profile

    Arguments:
    profile_id -- profile that owns the intervals
    """
    endpoint = '/link/4.0/data/intervals/{}/'.format(profile_id)

    return self.get(endpoint)

  def interval_retrieve(self, profile_id, meter_id, ym):
    """Return interval info - comes back as a CSV

    Arguments:
    profile_id -- profile that owns the interval
    meter_id -- meter of the interval
    ym -- year and month of the interval, i.e. YYYY-MM
    """
    endpoint = '/link/4.0/data/intervals/{}/{}/monthly/{}/'.format(profile_id, meter_id, ym)

    return self.get(endpoint)


class ProjectFetcher(LinkAPIClient):
  """Class for fetching all required project data into CSV files
  for Open EE datastore loading
  """

  def __init__(self, **kwargs):
    """Constructor"""

    super(ProjectFetcher, self).__init__(**kwargs)
    self.key = None

  def start_jobs(self, mode='interval'):
    """Starts jobs for all profiles and returns dictionary of successfully
    started jobs as well as jobs that failed to start

    Keyword Arguments:
    mode -- Type of job to start for all profiles, i.e. 'bill' (Default 'interval')
    """
    job_data = {
      "started": [],
      "failed": []
    }
    profile_response = self.profile_list()
    profiles = profile_response.data
    for profile in profiles:
      job_response = self.job_start(profile)
      if 200 <= job_response.status_code <= 202:
        job_id = job_response.data['job_id']
        job_data["started"].append({"profile": profile, "job_id": job_id})
      else:
        job_data["failed"].append(profile)
    return job_data
