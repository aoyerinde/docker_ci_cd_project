import unittest
from airflow.models import DagBag

class TestDagsValidity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def test_dags_loaded(self):
        """
        Validate that Airflow is able to import all DAGs in the repository
        """
        self.assertDictEqual(
            self.dagbag.import_errors,
            {},
            "DAG import errors: {}".format(self.dagbag.import_errors)
        )

    def test_alert_callback_set(self):
        """
        Validate that the Slack alert callback is set
        """
        for dag_id, dag in self.dagbag.dags.items():
            callback = dag.default_args.get('on_failure_callback', None)
            self.assertTrue(
                callback is not None and callback.__name__ == 'trigger_slack_alert',
                f'Slack alert callback not set for DAG {dag_id}'
            )

if __name__ == '__main__':
    unittest.main()
