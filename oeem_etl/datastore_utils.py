from oeem_etl.requester import Requester
from oeem_etl import config
from oeem_etl import constants


def loaded_project_ids():
    requester = Requester(config.oeem.url, config.oeem.access_token)
    response = requester.get(constants.PROJECT_ID_LIST_URL)
    return response.json()


def loaded_trace_ids():
    requester = Requester(config.oeem.url, config.oeem.access_token)
    response = requester.get(constants.TRACE_ID_LIST_URL)
    return response.json()
