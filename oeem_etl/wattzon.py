import requests
import json
import time
import base64
import csv

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
        """Initialize the host url with port/protocol, default to localhost.

        Parameters
        ----------
        host : str, optional
          Hostname to use
        port : int, optional
          Port to use
        protocol : str, optional
          Protocol to use (i.e. 'http')
        certificate : str, optional
          Path to certificate
        key : str, optional
          Path to private key
        **kwargs
          Arbitrary keyword args
        """
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
        """Returns URL built with host, port, and protocol

        Returns
        -------
        str
            Fully formed url string
        """
        return self.__url

    def build_uri(self, endpoint='/'):
        """Builds complete url with endpoint, host, port, and protocol

        Parameters
        ----------
        endpoint : str, optional
            Endpoint to build on

        Returns
        -------
        str
            Fully formed url string
        """
        if endpoint[-1] != '/':
            endpoint += '/'
        return self.url + endpoint

    def post(self, endpoint, data=None):
        """post for WattzOn endpoints

        Parameters
        ----------
        endpoint : str
            endpoint to post to
        data : str, optional
            data to post as payload

        Returns
        -------
        LinkResponse
        """
        url = self.build_uri(endpoint)
        payload = data
        response = requests.post(url, data=json.dumps(payload), cert=(self.__certificate, self.__key), **self.kwargs)
        return LinkResponse(response)

    def get(self, endpoint, data=None):
        """get for WattzOn endpoints

        Parameters
        ----------
        endpoint : str
            endpoint to get
        data : str, optional
            data to include in get

        Returns
        -------
        LinkResponse
        """
        url = self.build_uri(endpoint)
        query = data
        response = requests.get(url, params=query, cert=(self.__certificate, self.__key), **self.kwargs)
        return LinkResponse(response)

    def put(self, endpoint, data=None):
        """put for WattzOn endpoints

        Parameters
        ----------
        endpoint : str
            endpoint to put to
        data : str, optional
            data to put

        Returns
        -------
        LinkResponse
        """
        url = self.build_uri(endpoint)
        payload = data
        response = requests.put(url, data=json.dumps(payload), cert=(self.__certificate, self.__key), **self.kwargs)
        return LinkResponse(response)

    def delete(self, endpoint, data=None):
        """delete for WattzOn endpoints

        Parameters
        ----------
        endpoint : str
            endpoint to perform delete
        data : str, optional
            data to include in delete request

        Returns
        -------
        LinkResponse
        """
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

        Parameters
        ----------
        string : str
            The message that should be returned by echo

        Returns
        -------
        json
            json response
        """
        endpoint = '/link/4.0/echo/'
        data = {
            "string": string
        }

        return self.post(endpoint, data)

    def profile_list(self, id=None, provider=None, start_date=None, end_date=None, tags=None):
        """List profiles

        Parameters
        ----------
        id : int, optional
            Profile ID to filter (default None)
        provider : int, optional
            Provider ID to filter (default None)
        start_date : str, optional
            Minimum creation date to query (default None)
        end_date : str, optional
            Maximum creation date to query (default None)
        tags : str, optional
            Tags to filter, can be a list or comma separated string (default None)

        Returns
        -------
        LinkResponse
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

        Parameters
        ----------
        profile_id : int
            ID of profile to get

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/'.format(profile_id)
        return self.get(endpoint)

    def profile_create(self, provider_id, zipcode, label=None):
        """Create a profile

        Parameters
        ----------
        provider_id : int
            Provider id that will be associated to the profile
        zipcode : int
            Provider zipcode that will be associated to the profile
        label : str, optional
            Label to identify the profile (default None)

        Returns
        -------
        LinkResponse
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

        Parameters
        ----------
        profile_id : int
            ID of Profile to change
        zipcode : int, optional
            New zipcode (default None, don't change)
        label : str, optional
            The new label to identify the profile (default None, don't change)

        Returns
        -------
        LinkResponse
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

        Parameters
        ----------
        profile_id : int 
            ID of profile to delete

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/'.format(profile_id)

        return self.delete(endpoint)

    def key_get(self):
        """Returns public transmission key to encyrpt sensitive fields.
        To add sensitive fields you can also use profile_sensitive_add()

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/keys/'

        return self.get(endpoint)

    def _encrypt_value(self, value):
        """Helper function to encrypt sensitive fields

        Parameters
        ----------
        value : str
            Value to encrypt

        Returns
        -------
        str
            Encrypted value
        """
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

        Parameters
        ----------
        profile_id : int
            ID of profile to list sensitive fields

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/sensitive/'.format(profile_id)

        return self.get(endpoint)

    def profile_sensitive_add(self, profile_id, field, value):
        """Add a sensitive field to the profile

        Parameters
        ----------
        profile_id : int
            ID of profile to add sensitive field
        field : str 
            Name of the field to add
        value : str
            Value of the field to add

        Returns
        -------
        LinkResponse
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

        Parameters
        ----------
        profile_id : int
            ID of profile to change sensitive field
        field : str
            Name of field to change
        value : str
            Value to change the field to

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/sensitive/'.format(profile_id)
        data = {
            "field": field,
            "value": self._encrypt_value(value)
        }

        return self.put(endpoint, data)

    def profile_sensitive_delete(self, profile_id, field):
        """Deletes a sensitive field for the profile

        Parameters
        ----------
        profile_id : int
            ID of profile to delete sensitive field
        field : str
            Field to delete

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/sensitive/{}/'.format(profile_id, field)

        return self.delete(endpoint)

    def profile_tag_list(self, profile_id):
        """Lists tags associated with profile

        Parameters
        ----------
        profile_id : int 
            ID of profile to list the tags

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)

        return self.get(endpoint)

    def profile_tag_add(self, profile_id, name):
        """Adds tag to profile

        Parameters
        ----------
        profile_id : int 
            ID of profile to add tag
        name : str
            Name of tag to add

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)
        data = {
            "name": name
        }

        return self.post(endpoint, data)

    def profile_tag_delete(self, profile_id, name):
        """Deletes tag from profile

        Parameters
        ----------
        profile_id : int 
            ID of profile to delete tag
        name : str
            Name of tag to delete

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/profiles/{}/tags/'.format(profile_id)
        data = {
            "name": name
        }

        return self.delete(endpoint, data)

    def zip_info(self, zipcode):
        """Returns zipcode info

        Parameters
        ----------
        zipcode : int
            Zipcode to search

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/zips/{}/'.format(zipcode)

        return self.get(endpoint)

    def utility_find(self, zipcode=None, name=None, type=None):
        """Returns utility id for given arguments

        Parameters
        ----------
        zipcode : int, optional
            Zipcode of the provider (default None)
        name : str, optional
            Name of the provider (default None)
        type : str, optional
            Type of the provider, e.g. 'gas' (default None)

        Returns
        -------
        LinkResponse
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

        Parameters
        ----------
        provider_id : int
            ID of provider to search

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/utilities/{}/'.format(provider_id)

        return self.get(endpoint)

    def job_start(self, profile_id, mode='interval'):
        """Starts a new job for the profile

        Parameters
        ----------
        profile_id : int
            ID of profile to run job for
        mode : str, optional
            Mode of the WattzOn agent, e.g. 'bill' (default 'interval')

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/jobs/'
        data = {
            "profile_id": profile_id,
            "mode": mode
        }

        return self.post(endpoint, data)

    def job_status(self, job_id):
        """Get status of job

        Parameters
        ----------
        job_id : int
            ID of job to get status of

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/jobs/{}/'.format(job_id)

        return self.get(endpoint)

    def bill_get(self, profile_id, job_id=None):
        """Returns bill details

        Parameters
        ----------
        profile_id
            profile to get bills
        job_id : optional
            job id that extracted the bill (default None)

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/data/bills/{}/'.format(profile_id)
        data = {}

        if job_id:
            data.update({"job_id": job_id})

        return self.get(endpoint, data)

    def bill_file(self, profile_id, file_id):
        """Get bill pdf - file id can be found in bill_get()
        This doesn't actually work - would need to be properly handled as pdf download

        Paramters
        ---------
        profile_id
            profile that owns the bill pdf
        file_id
            file id to retrieve

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/data/bills/{}/{}/pdf/'.format(profile_id, file_id)

        return self.get(endpoint)

    def interval_list(self, profile_id):
        """List intervals available for the profile

        Parameters
        ----------
        profile_id
            profile that owns the intervals

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/data/intervals/{}/'.format(profile_id)

        return self.get(endpoint)

    def interval_retrieve(self, profile_id, meter_id, ym):
        """Return interval info - comes back as a CSV

        Parameters
        ----------
        profile_id
            profile that owns the interval
        meter_id
            meter of the interval
        ym
            year and month of the interval, i.e. YYYY-MM

        Returns
        -------
        LinkResponse
        """
        endpoint = '/link/4.0/data/intervals/{}/{}/monthly/{}/'.format(profile_id, meter_id, ym)

        return self.get(endpoint)


class LinkAPIHelper(LinkAPIClient):
    """Class for extending the basic Link API into additional, more helpful
    methods for Open EE purposes
    """

    def __init__(self, **kwargs):
        """Constructor"""
        super(LinkAPIHelper, self).__init__(**kwargs)
        self.key = None

    def start_jobs(self, mode='interval'):
        """Starts jobs for all profiles and returns dictionary of successfully
        started jobs as well as jobs that failed to start

        Parameters
        ----------
        mode : :obj:'str', optional
            Type of job to start for all profiles, i.e. 'bill' (Default 'interval')

        Returns
        -------
        str
            JSON formatted string containing successful and failed jobs in 
            following format:
            {
                "started": [
                    {
                        "profile": 1,
                        "job_id": "some_job_id"
                    },
                    {
                        "profile": 2,
                        "job_id": "some_other_job_id"
                    }
                ],
                "failed": [3, 4] 
            }

            Where 1-2 were successfully started and 3-4 were not.
        """
        job_data = {
            "started": [],
            "failed": []
        }
        profile_response = self.profile_list()
        profiles = profile_response.data
        for profile in profiles:
            job_response = self.job_start(profile)
            if job_response.success():
                job_id = job_response.data['job_id']
                job_data["started"].append({"profile": profile, "job_id": job_id})
            else:
                job_data["failed"].append(profile)
        return job_data

    def get_job_statuses(self, job_ids):
        """Gets all job statuses for a list of job ids, convenience instead of
        multiple calls yourself to job_status

        Parameters
        ----------
        job_ids : list of job ids

        Returns
        -------
        str
            JSON formatted lists of dictionaries containing the job status response
            broken down as success, failed, incomplete, or no_response.
            success -- job completed successfully at WattzOn
            failed -- job did not complete successfully at WattzOn
            incomplete -- job not finished at WattzOn
            no_response -- job did not yield a response from WattzOn
        """
        job_statuses = {
            "success": [],
            "failed": [],
            "incomplete": [],
            "no_response": []
        }
        for job_id in job_ids:
            job_response = self.job_status(job_id)
            if job_response.success():
                finished = job_response.data["finished"]
                if finished:
                    if job_response.data["job_status"] == "AgentState.success":
                        job_statuses["success"].append(job_response.data)
                    else:
                        job_statuses["failed"].append(job_response.data)
                else:
                    job_statuses["incomplete"].append(job_response.data)
            else:
                job_statuses["no_response"].append({"job_id":job_id, "status_code":job_response.status_code})
        return job_statuses        

    def get_interval_json(self, profile_id, meter_id, ym):
        """Gets interval data but converts the raw CSV response to JSON to
        match other types of responses from the service

        Parameters
        ----------
        profile_id : id of the profile for which to get interval
        meter_id : id of meter for which to get interval
        ym : Year and month (YYYY-MM format) for which to get interval

        Returns
        -------
        str
            JSON formatted string with a "success" boolean indicating service
            was successfully contacted and an "interval_data" section containing
            converted CSV data
        """
        response = self.interval_retrieve(profile_id, meter_id, ym)
        data = {
            "success": "false",
            "interval_data": {}
        }
        if response.success():
            data['success'] = "true"
            #raw_lines = str(response.data).split('\n')
            #good_lines = ()
            #for line in raw_lines:
            #    if line.startswith('')
            data['interval_data'] = response.data
        else:
            data['status'] = "false"
            data['interval_data'] = response.status_code

        return data
