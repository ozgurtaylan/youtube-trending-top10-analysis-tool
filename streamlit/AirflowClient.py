import airflow_client.client as client
from airflow_client.client.api import dag_run_api
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.error import Error
from pprint import pprint


class Client():
    
    def __init__(self) -> None:
        pass

    airflow_configuration = client.Configuration(
        host = "http://airflow-webserver:8080/api/v1",
        username = 'airflow',
        password = 'airflow'
    )

    def start_external_triger(self):
        # Enter a context with an instance of the API client
        with client.ApiClient(self.airflow_configuration) as api_client:
            # Create an instance of the API class
            api_instance = dag_run_api.DAGRunApi(api_client)
            dag_id = "youtube_data_pipeline" 
            dag_run = DAGRun(
                conf={},
                note="external trigger!",
        )
        try:
            api_response = api_instance.post_dag_run(dag_id, dag_run)
            pprint(api_response)
        except client.ApiException as e:
            print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)