import unittest
from airflow.models import DagBag

class TestDagSerialization(unittest.TestCase):
    def test_dag_import_errors(self):
        """
        Kiểm tra xem các DAG có lỗi import không.
        Đây là bước bắt buộc trong CI/CD để đảm bảo không deploy code lỗi lên Airflow.
        """
        dagbag = DagBag(dag_folder='dags/', include_examples=False)
        self.assertFalse(
            len(dagbag.import_errors) > 0,
            f"Phát hiện lỗi Import trong DAG: {dagbag.import_errors}"
        )

if __name__ == "__main__":
    unittest.main()
